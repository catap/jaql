/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.io.index;

import java.io.Closeable;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.util.BaseUtil;
import com.ibm.jaql.util.BufferedRandomAccessFile;
import com.ibm.jaql.util.LongArray;

public class JIndexReader implements Closeable
{
  private String filename;
  private long fileVersion;
  private BufferedRandomAccessFile base;
  private ArrayList<Index> indexes = new ArrayList<Index>();
  private ArrayList<JsonValue> root;
  private LongArray rootp;
  private JsonValue key;
  private JsonValue value;
  private BufferedJsonArray tuple = new BufferedJsonArray(2);
//  private Item result = new Item(tuple);
  private JsonValue minKey;
  private JsonValue maxKey;
  private long numIndexes;
  private long minOffset;

  private BinaryFullSerializer serializer = DefaultBinaryFullSerializer.getInstance();
  
  /**
   * 
   * @param filename
   * @throws IOException
   */
  public JIndexReader(String filename) throws IOException
  {
    this.filename = filename;
    readSummary();    
    base = new BufferedRandomAccessFile(filename+".base", "r", 4096); // TODO: how to set buffer?
    readHeader(base, JIndexWriter.BASE_FILE);
    minOffset = base.getFilePointer();
  }
  
  /**
   * 
   * @throws IOException
   */
  public void close() throws IOException
  {
    base.close();
    for(Index index: indexes)
    {
      index.in.close();
    }
  }
  
  /**
   * Find all (key,value)-pairs between low and high.
   * 
   * This code is NOT safe to produce multiple simultaneous scans! // TODO: improve this?
   * You must open multiple JIndexReaders to do that at this time.
   * 
   * @param low minimum value to include in scan, or null for no min
   * @param high maximum value to include in scan, or null for no max
   * @return
   */
  public JsonIterator rangeScan(final JsonValue low, final JsonValue high) throws IOException
  {
    if( low == null )
    {
      base.seek(minOffset);
    }
    else
    {
      if( root == null )
      {
        loadIndexes();
      }
      int p = Collections.binarySearch(root, low);
      if( p < 0 ) // key not found in index
      {
        p = -p - 1; 
        // root[p-1] < low < root[p] (if p-1 >= 0 && p < root.size())
        if( p == root.size() ) // p-1 = last, root[last] < low 
        {
          if( low.compareTo(maxKey) > 0 ) // root[last] < max < low
          {
            return JsonIterator.EMPTY;            // no results
          }
          // root[p-1] < low <= max
        }
        // else p == 0, low < root[0], so use first index entry
        // because root[0] is minKey in file
      }
      else // key is in index
      {
        p++;
      }
      // p is the index of the first key such that root[p] <= low, or 0 if low < root[0]
      long offset = rootp.get(p);
      offset = indexLookup(indexes.size() - 2, offset, low);
      base.seek(offset);
      try
      {
        key = serializer.read(base, key);
        while( key.compareTo(low) < 0 )
        {
          value = serializer.read(base, value);
          offset = base.getFilePointer();
          key = serializer.read(base, key);
        }
        base.seek(offset);
      }
      catch(EOFException ex)
      {
        return JsonIterator.EMPTY;
      }
    }

    return new JsonIterator(tuple)
    {
      @Override
      public boolean moveNext() throws Exception
      {
        try
        {
          key = serializer.read(base, key);
          value = serializer.read(base, value);
          if( high == null || key.compareTo(high) <= 0 )
          {
            tuple.set(0, key);
            tuple.set(1, value);
            return true; // currentValue == tuple
          }
        }
        catch(EOFException e) {}
        base.seek(base.length()); // just to be safe in case moveNext() is called again
        return false; 
      }
    };
  }
    
  private void loadIndexes() throws IOException
  {
    root = new ArrayList<JsonValue>();
    rootp = new LongArray();

    for(int i = 0 ; i < numIndexes ; i++)
    {
      Index index = new Index(filename, i);
      indexes.add(index);
    }
    Index index = indexes.get(indexes.size()-1);
    try
    {
      rootp.add(index.prevMinOffset);
      while( true )
      {
        JsonValue k = serializer.read(index.in, null);
        root.add(k);
        long offset = BaseUtil.readVULong(index.in);
        rootp.add(offset);
      }
    }
    catch( EOFException ex ) {}
  }
  
  private long indexLookup(int i, long offset, final JsonValue low) throws IOException
  {
    for( ; i >= 0 ; i--)
    {
      try
      {
        Index index = indexes.get(i);
        index.in.seek(offset);
        offset = index.prevMinOffset;
        key = serializer.read(index.in, key);
        int c;
        while( (c = low.compareTo(key)) > 0 )
        {
          offset = BaseUtil.readVULong(index.in);
          key = serializer.read(index.in, key);
        }
        if( c == 0 )
        {
          offset = BaseUtil.readVULong(index.in);
        }
      }
      catch( EOFException ex ) {}
    }
    return offset;
  }

  private void readHeader(DataInput in, long fileType) throws IOException
  {
    long x;
    x = BaseUtil.readVULong(in);
    if( x != JIndexWriter.ENCODING_VERSION )
    {
      throw new IOException("Invalid index encoding version: "+x+" expected: "+JIndexWriter.ENCODING_VERSION);
    }
    x = BaseUtil.readVULong(in);
    if( fileVersion == 0 )
    {
      fileVersion = x;
    }
    else if( x != fileVersion )
    {
      throw new IOException("Invalid index version: "+x+" expected: "+fileVersion);
    }
    x = BaseUtil.readVULong(in);
    if( x != fileType )
    {
      throw new IOException("Invalid index file type: "+x+" expected: "+fileType);
    }
  }
  
  private void readSummary() throws IOException
  {
    BufferedRandomAccessFile summary =  new BufferedRandomAccessFile(filename+".summary", "r", 4096);
    readHeader(summary, JIndexWriter.SUMMARY_FILE);
    
    minKey = serializer.read(summary, minKey);
    maxKey = serializer.read(summary, maxKey);
    /*totalItems =*/ BaseUtil.readVULong(summary);
    numIndexes = BaseUtil.readVULong(summary);

    long x = BaseUtil.readVULong(summary);
    if( x != JIndexWriter.SUCCESS )
    {
      throw new IOException("invalid index summary file indicator: "+x+" expected: "+JIndexWriter.SUCCESS);
    }

    summary.close();
  }

  class Index
  {
    BufferedRandomAccessFile in;
    long prevMinOffset;
    
    public Index(String loc, int level) throws IOException
    {
      in = new BufferedRandomAccessFile(loc+".idx"+level, "r", 1024); // TODO: how to set buffer?
      readHeader(in, JIndexWriter.INDEX_FILE);
      long x = BaseUtil.readVULong(in);
      if( x != level )
      {
        throw new IOException("invalid index file level: "+x+" expected: "+level);
      }
      prevMinOffset = level == 0 ? minOffset : in.getFilePointer(); 
    }
  }
}
