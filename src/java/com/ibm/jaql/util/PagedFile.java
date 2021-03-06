/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ConcurrentModificationException;

/** 
 * Methods to access a file consisting of a set of pages, including methods to 
 * allocate, read, write, and free a page. The physical order of pages on disk may not 
 * correspond to the order of their allocation, as freed pages will be reused to 
 * satisfy subsequent allocations. 
 * 
 * Jaql uses a single page file per JVM, which can be retrieved using 
 * {@link PagedFile#get()}. The page file is cleaned up when the JVM exits.
 */
public final class PagedFile
{
  public static final int allocSize = 10; // number of pages to allocate at a time

  protected FileChannel   file;
  protected ByteBuffer    freeList;	// list of free pages
  protected long          fileEnd;
  protected int           version;

  private static final int       TEMP_PAGE_SIZE           = 64 * 1024;    // TODO: this is tuneable
  private static final PagedFile pagedFile = makeTempPagedFile(); 
  
  /** Return the global page file. */
  public static PagedFile get()
  {
    return pagedFile;
  }
  
  /**
   * @param prefix
   * @return
   */
  public static PagedFile makeTempPagedFile()
  {
    try
    {
      File f = File.createTempFile("jaql-pagefile", ".dat");
      f.deleteOnExit();
      RandomAccessFile file = new RandomAccessFile(f, "rw");
      file.setLength(0);
      PagedFile pf = new PagedFile(file.getChannel(), TEMP_PAGE_SIZE);
      return pf;
    }
    catch (IOException ex)
    {
      throw new UndeclaredThrowableException(ex);
    }
  }
  
  /**
   * pageSize must be a multiple of 8, >= 256, and acceptable to ByteBuffer
   * 
   * @param file
   * @param pageSize
   * @throws IOException
   */
  public PagedFile(FileChannel file, int pageSize) throws IOException
  {
    this.file = file;
    freeList = ByteBuffer.allocate(pageSize);
    if (file.size() >= pageSize)
    {
      // read the file header
      // The file header is only the following, but we can add more later:
      //    int pageSize
      //    long fileEnd
      //    long freeListHead
      //    int freeLimit (bytes used on firstListHead) 
      freeList.clear();
      file.read(freeList, 0);
      int storedPageSize = freeList.getInt();
      if (pageSize <= 0)
      {
        pageSize = storedPageSize;
      }
      else if (storedPageSize != pageSize)
      {
        throw new IOException("invalid page size: " + pageSize + " != "
            + storedPageSize);
      }
      fileEnd = freeList.getLong();
      long freeListHead = freeList.getLong();
      int freeLimit = freeList.getInt();
      // read the start of the freeList
      freeList.clear();
      file.read(freeList, freeListHead);
      freeList.position(freeLimit);
      freeList.limit(pageSize);
    }
    else
    {
      assert pageSize >= 128 && pageSize % 8 == 0;
      clear();
    }
  }

  /**
   * 
   * @return
   */
  public int getVersion()
  {
    return this.version;
  }

  /** 
   * @return
   */
  public int pageSize()
  {
    return freeList.capacity();
  }

  /**
   * 
   */
  public synchronized void clear()
  {
    if (this == get())
    {
      throw new IllegalStateException("global page file must not be cleared");
    }
    
    // init the freeList
    version++;
    fileEnd = pageSize(); // the first page is the file header
    freeList.clear();
    freeList.putLong(-1);
  }

  /**
   * @throws IOException
   */
  public synchronized void close() throws IOException
  {
    int n = freeList.position();
    long freeListHead;
    // if the freeList has at least one item on it
    if (n > 8)
    {
      // write the freeList to the last free page
      n -= 8;
      freeListHead = freeList.getLong(n);
      freeList.clear();
      file.write(freeList, freeListHead);
    }
    else
    {
      // point to the free list page on disk, which is full of pointers
      freeListHead = freeList.getLong(0);
      n = pageSize();
    }
    // write the file header
    freeList.clear();
    freeList.putInt(pageSize());
    freeList.putLong(freeListHead);
    freeList.putInt(n); // bytes on the last free list page
    freeList.clear();
    file.write(freeList, 0);
  }

  /** Adds a page to the list of free pages.
   * @param offset
   * @throws IOException
   */
  public synchronized int freePage(int expectedVersion, long offset) throws IOException
  {
    if( expectedVersion == version )
    {
      if (freeList.hasRemaining())
      {
        // If there is room in the in-memory free list, add this page there 
        freeList.putLong(offset);
      }
      else
      {
        // If there is NO room in the in-memory free list, write the current free list to this page 
        freeList.clear();
        file.write(freeList, offset);
        freeList.clear();
        freeList.putLong(offset); // next page of free page pointers is this page
      }
    }
    return version;
  }

  /** Clears this file. Implemented by reading and then freeing all used pages, which might 
   * be expensive. 
   * @throws IOException */
  // TODO: free page directory could be head of linked list of pages, or SpillFile can use page directories 
  public int freePages(int expectedVersion, long firstPage, ByteBuffer buffer)
    throws IOException
  {
    if( expectedVersion == version )
    {
      for (long page = firstPage; page >= 0; page = buffer.getLong(0))
      {
        read(version, buffer, page);
        freePage(version, page);
      }
    }
    return version;
  }

  /**
   * @return
   * @throws IOException
   */
  public synchronized long allocatePage(int expectedVersion) throws IOException
  {
    if( expectedVersion != version )
    {
      throw new ConcurrentModificationException("expected version " + expectedVersion 
          + " but found " + version);
    }
    int n = freeList.position() - 8;
    assert n >= 0;
    if (n == 0)
    {
      // no more free blocks on the list
      long nextFree = freeList.getLong(0);
      if (nextFree >= 0)
      {
        // read the next (full) page of free page pointers
        freeList.clear();
        file.read(freeList, nextFree);
      }
      else
      {
        // no more free blocks - extend the file
        long pageSize = pageSize();
        fileEnd += allocSize * pageSize;
        long page = fileEnd;
        for (int i = 0; i < allocSize; i++)
        {
          page -= pageSize;
          freePage(expectedVersion, page);
        }
      }
      n = freeList.position() - 8;
      assert n > 0;
    }

    // take last pointer from the current freeList buffer
    long p = freeList.getLong(n);
    assert p >= pageSize(); // the first page is the file file header
    freeList.position(n);
    return p;
  }

  /**
   * @param buffer
   * @param offset
   * @throws IOException
   */
  public void write(int expectedVersion, ByteBuffer buffer, long offset) throws IOException
  {
    if( expectedVersion != version )
    {
      throw new ConcurrentModificationException("expected version " + expectedVersion 
          + " but found " + version);
    }
    buffer.clear(); // write the whole page
    file.write(buffer, offset);
  }

  /**
   * @param buffer
   * @return
   * @throws IOException
   */
  public long write(int expectedVersion, ByteBuffer buffer) throws IOException
  {
    long offset = allocatePage(expectedVersion);
    buffer.clear();
    file.write(buffer, offset);
    return offset;
  }

  /**
   * @param buffer
   * @param offset
   * @throws IOException
   */
  public void read(int expectedVersion, ByteBuffer buffer, long offset) throws IOException
  {
    if( expectedVersion != version )
    {
      throw new ConcurrentModificationException("expected version " + expectedVersion 
          + " but found " + version);
    }
    buffer.clear();
    file.read(buffer, offset);
    buffer.position(0);
  }

}
