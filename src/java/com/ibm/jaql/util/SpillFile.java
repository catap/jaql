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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;

/** 
 * Write-once DataOutput backed by a {@link PagedFile}. A single page file can serve multiple 
 * spill files. Spill files are intended to be used as temporary files; reading the spill 
 * file requires the SpillFile instance that created it. 
 * 
 * This class is not thread-safe.
 */
public class SpillFile implements DataOutput
{
  protected final static int headerSize  = 16;
  protected ByteBuffer       buffer; // contains the current page
  protected boolean          frozen;
  protected int              version     = 0;
  protected long             firstPage   = -1;
  protected long             currentPage = -1;
  protected long             fileSize    = 0;
  protected PagedFile        file;
  protected int              fileVersion;
  private byte[]             byteBuf     = new byte[8];

  /** Creates a new, empty spill file backed by the given PagedFile. */
  public SpillFile(PagedFile file)
  {
    this.file = file;
    this.fileVersion = file.getVersion();
    buffer = ByteBuffer.allocate(file.pageSize());
    try
    {
      clear();
    }
    catch (IOException ex)
    {
      throw new UndeclaredThrowableException(ex, "this should not have happened...");
    }
  }

  /** Write the header to the beginning of the page. */
  private void putHeader(long nextPage)
  {
    buffer.putLong(0, nextPage);
    buffer.putInt(8, buffer.position()); // used bytes
    buffer.putInt(12, 0); // unused
  }

  /** Initialize the buffered page.  */
  private void initPage(long nextPage)
  {
    buffer.clear();
    buffer.position(headerSize);
    putHeader(-1);
  }

  /** Returns the file offset of the next page. */
  protected final long getNextPage()
  {
    return buffer.getLong(0);
  }

  /**
   * @return
   */
  protected final int getUsedBytes()
  {
    return buffer.getInt(8);
  }

  public void clear() throws IOException
  {
    fileVersion = file.freePages(fileVersion, firstPage, buffer);
    frozen = false;
    firstPage = currentPage = -1;
    initPage(-1);
    fileSize = 0;
    version++;
  }
  
  @Override
  protected void finalize() throws Throwable {
    file.freePages(fileVersion, firstPage, buffer);
  }

  
  public void setFile(PagedFile file) throws IOException
  {
    clear();
    if( this.file.pageSize() != file.pageSize() )
    {
      buffer = ByteBuffer.allocate(file.pageSize()); 
    }
    this.file = file;
  }

  /** Write the buffered page to disk and finalize the file. After freezing, no further 
   * modifications with the exception of {@link #clear()} should be performed on this file. */
  public void freeze() throws IOException
  {
    assert !frozen;
    putHeader(-1); // no next page
    fileSize += buffer.position() - headerSize;
    if (firstPage >= 0)
    {
      // write the last page
      file.write(fileVersion, buffer, currentPage);
    }
    frozen = true;
  }

  /** Write the buffered page to disk, allocate a new page, and set the current page to 
   * the newly allocated page */
  private void writeBuffer() throws IOException
  {
    assert !frozen;
    if (currentPage == -1)
    {
      assert firstPage == -1;
      firstPage = currentPage = file.allocatePage(fileVersion);
    }
    long nextPage = file.allocatePage(fileVersion);
    int usedBytesOnPage = buffer.position() - headerSize;
    fileSize += usedBytesOnPage;
    putHeader(nextPage);
    file.write(fileVersion, buffer, currentPage);
    currentPage = nextPage;
    initPage(-1);
  }

  /**
   * @return
   * @throws IOException
   */
  public SFDataInput getInput() throws IOException
  {
    return new SFDataInput();
  }

  /**
   * 
   */
  // makes use of the same buffer as the writer
  public class SFDataInput implements DataInput
  {
    protected long rpage;
    protected int  rpos;
    protected int  limit;
    protected int  rversion;

    /**
     * @throws IOException
     */
    public SFDataInput() throws IOException
    {
      rewind();
    }
    
    private SFDataInput(SFDataInput other) {
      rpage = other.rpage;
      rpos = other.rpos;
      limit = other.limit;
      rversion = other.rversion;
    }
    
    /** Reset the input to the beginning of the file.
     * @throws IOException
     */
    public void rewind() throws IOException
    {
      assert frozen;
      rversion = version;
      rpage = firstPage;
      if (currentPage != rpage)
      {
        currentPage = rpage;
        file.read(fileVersion, buffer, rpage);
      }
      rpos = headerSize;
      limit = getUsedBytes();
    }

    /**
     * returns with: if eof throw EOFException else rpage may advance to the
     * next page rpos < limit currentPage = rpage buffer has the contents of
     * rpage don't rely one buffer.limit or buffer.position
     * 
     * @throws IOException
     */
    private void reposition() throws IOException
    {
      assert frozen && rversion == version;
      if (rpage != currentPage)
      {
        currentPage = rpage;
        file.read(fileVersion, buffer, rpage);
      }
      if (rpos >= limit)
      {
        // follow the next pointer
        rpage = getNextPage(); // next page
        if (rpage < 0)
        {
          throw new EOFException();
        }
        // read the next page
        currentPage = rpage;
        file.read(fileVersion, buffer, rpage);
        rpos = headerSize;
        limit = getUsedBytes();
      }
      if (rpos == limit)
      {
        throw new EOFException();
      }
      assert limit > rpos;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readByte()
     */
    public byte readByte() throws IOException
    {
      reposition();
      byte b = buffer.get(rpos);
      rpos++;
      return b;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readFully(byte[], int, int)
     */
    public void readFully(byte[] bytes, int off, int len) throws IOException
    {
      while (len > 0)
      {
        reposition();
        int n = limit - rpos;
        if (n > len)
        {
          n = len;
        }
        buffer.position(rpos);
        buffer.get(bytes, off, n);
        rpos += n;
        len -= n;
        off += n;
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#skipBytes(int)
     */
    public int skipBytes(int len) throws IOException
    {
      int nread = 0;
      while (len > 0)
      {
        reposition();
        int n = limit - rpos;
        if (n > len)
        {
          n = len;
        }
        rpos += n;
        len -= n;
        nread += n;
      }
      return nread;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readFully(byte[])
     */
    public void readFully(byte[] bytes) throws IOException
    {
      readFully(bytes, 0, bytes.length);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readBoolean()
     */
    public boolean readBoolean() throws IOException
    {
      byte b = readByte();
      return b != 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readChar()
     */
    public char readChar() throws IOException
    {
      readFully(byteBuf, 0, 2);
      char c = (char) (((byteBuf[0] & 0xff) << 8) | (byteBuf[1] & 0xff));
      return c;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readDouble()
     */
    public double readDouble() throws IOException
    {
      long x = readLong();
      return Double.longBitsToDouble(x);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readFloat()
     */
    public float readFloat() throws IOException
    {
      int x = readInt();
      return Float.intBitsToFloat(x);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readInt()
     */
    public int readInt() throws IOException
    {
      readFully(byteBuf, 0, 4);
      int x = ((byteBuf[0] & 0xff) << 24) | ((byteBuf[1] & 0xff) << 16)
          | ((byteBuf[2] & 0xff) << 8) | ((byteBuf[3] & 0xff));
      return x;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readLong()
     */
    public long readLong() throws IOException
    {
      readFully(byteBuf, 0, 8);
      // tricky code, be careful when modifying
      long x = (((long) byteBuf[0] << 56) + ((long) (byteBuf[1] & 255) << 48)
          + ((long) (byteBuf[2] & 255) << 40)
          + ((long) (byteBuf[3] & 255) << 32)
          + ((long) (byteBuf[4] & 255) << 24) + ((byteBuf[5] & 255) << 16)
          + ((byteBuf[6] & 255) << 8) + ((byteBuf[7] & 255) << 0));
      return x;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readShort()
     */
    public short readShort() throws IOException
    {
      readFully(byteBuf, 0, 2);
      short x = (short) ((byteBuf[0] << 8) | (byteBuf[1] & 0xff));
      return x;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readUnsignedByte()
     */
    public int readUnsignedByte() throws IOException
    {
      readFully(byteBuf, 0, 1);
      return byteBuf[0] & 0xff;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readUnsignedShort()
     */
    public int readUnsignedShort() throws IOException
    {
      readFully(byteBuf, 0, 2);
      int x = ((byteBuf[0] & 0xff) << 8) | (byteBuf[1] & 0xff);
      return x;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readLine()
     */
    public String readLine() throws IOException
    {
      throw new UnsupportedOperationException("readLine is NYI");
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.io.DataInput#readUTF()
     */
    public String readUTF() throws IOException
    {
      int len = readShort();
      if (byteBuf.length < len)
      {
        byteBuf = new byte[len];
      }
      readFully(byteBuf, 0, len);
      String s = new String(byteBuf, 0, len);
      return s;
    }

    /** Make a copy of this input stream. Can be used to remember positions in the input file. */ 
    public SFDataInput getCopy() {
      return new SFDataInput(this);
    }
  }

  /**
   * @return
   */
  public long size()
  {
    return fileSize;
  }

  /** Writes the content of this (frozen!) spill file to the provided DataOutput. 
   * @param out
   * @throws IOException
   */
  public void writeToOutput(DataOutput out) throws IOException
  {
    assert frozen;
    if (currentPage != firstPage)
    {
      currentPage = firstPage;
      file.read(fileVersion, buffer, currentPage);
    }
    out.write(buffer.array(), buffer.arrayOffset() + headerSize, getUsedBytes()
        - headerSize);
    for (long page = getNextPage(); page >= 0; page = getNextPage())
    {
      currentPage = page;
      file.read(fileVersion, buffer, page);
      out.write(buffer.array(), buffer.arrayOffset() + headerSize,
          getUsedBytes() - headerSize);
    }
  }

  /** Clear this spill file, copy the content of the provided spill file into this spill
   * file, and then freeze it. */
  public void copy(SpillFile spill) throws IOException
  {
    clear();
    spill.writeToOutput(this);
    freeze();
  }

  /** Appends len bytes from in to this (non-frozen!) spill file.
   * @param in
   * @param len
   * @throws IOException
   */
  public void writeFromInput(DataInput in, long len) throws IOException
  {
    assert !frozen;
    int n;
    while (len > (n = buffer.remaining()))
    {
      int p = buffer.position();
      in.readFully(buffer.array(), buffer.arrayOffset() + p, n);
      buffer.position(p + n);
      writeBuffer();
      len -= n;
    }
    n = (int) len;
    if (n > 0)
    {
      in.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(), n);
      buffer.position(buffer.position() + n);
    }
  }

  //---------------------
  // DataOutput methods
  //---------------------

  /* 
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#write(byte[], int, int)
   */
  public void write(byte[] bytes, int off, int len) throws IOException
  {
    assert !frozen;
 
    while (len > 0)
    {
      int n = buffer.remaining();    
      if (n >= len)
      {
        buffer.put(bytes, off, len);
        break;
      }
      buffer.put(bytes, off, n);
      len -= n;
      off += n;
      writeBuffer();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#write(int)
   */
  public void write(int b) throws IOException
  {
    byteBuf[0] = (byte) b;
    write(byteBuf, 0, 1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#write(byte[])
   */
  public void write(byte[] bytes) throws IOException
  {
    write(bytes, 0, bytes.length);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#writeBoolean(boolean)
   */
  public void writeBoolean(boolean bool) throws IOException
  {
    write(bool ? (byte) 1 : (byte) 0);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#writeByte(int)
   */
  public void writeByte(int b) throws IOException
  {
    write(b);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#writeChar(int)
   */
  public void writeChar(int c) throws IOException
  {
    byteBuf[0] = (byte) ((c >> 8) & 0xff);
    byteBuf[1] = (byte) (c & 0xff);
    write(byteBuf, 0, 2);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#writeInt(int)
   */
  public void writeInt(int i) throws IOException
  {
    byteBuf[0] = (byte) ((i >> 24) & 0xff);
    byteBuf[1] = (byte) ((i >> 16) & 0xff);
    byteBuf[2] = (byte) ((i >> 8) & 0xff);
    byteBuf[3] = (byte) (i & 0xff);
    write(byteBuf, 0, 4);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#writeLong(long)
   */
  public void writeLong(long i) throws IOException
  {
    byteBuf[0] = (byte) ((i >> 56) & 0xff);
    byteBuf[1] = (byte) ((i >> 48) & 0xff);
    byteBuf[2] = (byte) ((i >> 40) & 0xff);
    byteBuf[3] = (byte) ((i >> 32) & 0xff);
    byteBuf[4] = (byte) ((i >> 24) & 0xff);
    byteBuf[5] = (byte) ((i >> 16) & 0xff);
    byteBuf[6] = (byte) ((i >> 8) & 0xff);
    byteBuf[7] = (byte) (i & 0xff);
    write(byteBuf, 0, 8);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#writeShort(int)
   */
  public void writeShort(int i) throws IOException
  {
    byteBuf[0] = (byte) ((i >> 8) & 0xff);
    byteBuf[1] = (byte) (i & 0xff);
    write(byteBuf, 0, 2);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#writeDouble(double)
   */
  public void writeDouble(double d) throws IOException
  {
    writeLong(Double.doubleToRawLongBits(d));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#writeFloat(float)
   */
  public void writeFloat(float f) throws IOException
  {
    writeInt(Float.floatToRawIntBits(f));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#writeBytes(java.lang.String)
   */
  public void writeBytes(String s) throws IOException
  {
    // This could be faster...
    for (int i = 0; i < s.length(); i++)
    {
      writeByte(s.charAt(i));
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#writeChars(java.lang.String)
   */
  public void writeChars(String s) throws IOException
  {
    // This could be faster...
    for (int i = 0; i < s.length(); i++)
    {
      writeChar(s.charAt(i));
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.DataOutput#writeUTF(java.lang.String)
   */
  public void writeUTF(String s) throws IOException
  {
    // This could be faster...
    int utfLen = 0;
    for (int i = 0; i < s.length(); i++)
    {
      char c = s.charAt(i);
      if (c <= '\u007f' && c > 0)
      {
        utfLen++;
      }
      else if (c <= '\u07ff')
      {
        utfLen += 2;
      }
      else
      {
        utfLen += 3;
      }
    }
    if (utfLen > 65535)
    {
      throw new UTFDataFormatException("string too long");
    }
    writeShort(utfLen);
    for (int i = 0; i < s.length(); i++)
    {
      char c = s.charAt(i);
      if (c <= '\u007f' && c > 0)
      {
        writeByte(c);
      }
      else if (c <= '\u07ff')
      {
        writeByte(0xc0 | (0x1f & (c >> 6)));
        writeByte(0x80 | (0x3f & c));
      }
      else
      {
        writeByte(0xe0 | (0x0f & (c >> 12)));
        writeByte(0x80 | (0x3f & (c >> 6)));
        writeByte(0x80 | (0x3f & c));
      }
    }
  }

  /**
   * @return
   */
  public final boolean isFrozen()
  {
    return frozen;
  }
}
