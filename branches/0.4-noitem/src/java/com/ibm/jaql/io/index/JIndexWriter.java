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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

public final class JIndexWriter implements Closeable
{
  public static final long ENCODING_VERSION = 1;
  public static final long SUMMARY_FILE = 1;
  public static final long BASE_FILE = 2;
  public static final long INDEX_FILE = 3;
  public static final long SUCCESS = 17;
  
  private static final int indexSkip = 100;
  private static final int baseSkip = 1;
  private String filename;
  private DataOutputStream base;
  private DataOutputStream summary;
  private ArrayList<Index> indexes = new ArrayList<Index>();
  private long totalItems = 0;
  private long baseItems = 0;
  private long fileVersion;
  private JsonValue prevKey = null;
  
  private BinaryFullSerializer serializer = DefaultBinaryFullSerializer.getInstance();
  
  public JIndexWriter(String filename) throws IOException
  {
    this.filename = filename;
    this.fileVersion = System.currentTimeMillis();
    FileOutputStream fos  = new FileOutputStream(filename+".base");
    base = new DataOutputStream(new BufferedOutputStream(fos));
    writeHeader(base, BASE_FILE);
    
    fos  = new FileOutputStream(filename+".summary");
    summary = new DataOutputStream(new BufferedOutputStream(fos));
    writeHeader(summary, SUMMARY_FILE);
    
    // Delete old index files (assumes all 0..n are present)
    // TODO: this would be better using a filename.idx*
    int i = 0;
    File file = new File(filename+".idx"+i);
    while( file.exists() )
    {
      if( !file.delete() )
      {
        throw new IOException("couldn't delete index file: "+file);
      }
      i++;
      file = new File(filename+".idx"+i);
    }
    
    indexes.add(new Index(filename, 0));
    indexes.add(new Index(filename, 1));
  }
  
  /**
   * Add and (key,value)-pair to the file.  The keys MUST be added in order!
   * 
   * @param key
   * @param value
   * @throws IOException
   */
  public void add(JsonValue key, JsonValue value) throws Exception
  {
    if( totalItems == 0 )
    {
      serializer.write(summary, key);
      baseItems++;
      if( baseItems >= baseSkip )
      {
        writeIndex(0,key,base.size());
        baseItems = 0;
      }
    }
    else
    {
      baseItems++;
      if( baseItems >= baseSkip && key.compareTo(prevKey) != 0 )
      {
        writeIndex(0,key,base.size());
        baseItems = 0;
      }
    }
    prevKey.setCopy(key);
    serializer.write(base, key);
    serializer.write(base, value);
    totalItems++;
  }
  
  /**
   * Close the writer.
   * 
   * @throws IOException
   */
  public void close() throws IOException
  {
    // prune tiny indexes away
    while( true )
    {
      int n = indexes.size();
      Index index = indexes.get(n-1);
      if( n == 1 || index.numKeys >= indexSkip / 2 )
      {
        break;
      }
      index.out.close();
      if( ! index.file.delete() )
      {
        throw new IOException("index couldn't be deleted: "+index.file);
      }
      indexes.remove(n-1);
    }

    if( totalItems == 0 ) // if empty file, write null for min key
    {
      serializer.write(summary, prevKey);
    }
    serializer.write(summary, prevKey); // write max key
    BaseUtil.writeVULong(summary, totalItems);
    BaseUtil.writeVULong(summary, indexes.size());

    for(Index index: indexes)
    {
      index.out.close();
    }

    base.close();

    BaseUtil.writeVULong(summary, SUCCESS);
    summary.close();
  }
  
  private void writeIndex(int level, JsonValue key, long offset) throws IOException
  {
    Index index0 = indexes.get(level);
    Index index1 = indexes.get(level+1);
    index1.counter++;
    if( index1.counter == indexSkip )
    {
      if( indexes.size() == level + 2 )
      {
        indexes.add(new Index(filename,level+2));
      }
      writeIndex(level+1, key, index0.out.size());
    }
    serializer.write(index0.out, key);
    BaseUtil.writeVULong(index0.out, offset);
    index0.numKeys++;
    index0.counter = 0;
  }
  
  private void writeHeader(DataOutputStream out, long fileType) throws IOException
  {
    BaseUtil.writeVULong(out, ENCODING_VERSION);
    BaseUtil.writeVULong(out, fileVersion);
    BaseUtil.writeVULong(out, fileType);
  }

  class Index
  {
    int counter;
    long numKeys;
    File file;
    FileOutputStream fos;
    DataOutputStream out;
    
    public Index(String loc, int level) throws IOException
    {
      file = new File(loc+".idx"+level);
      fos = new FileOutputStream(file);
      out = new DataOutputStream(new BufferedOutputStream(fos));
      writeHeader(out, INDEX_FILE);
      BaseUtil.writeVULong(out, level);
    }
  }

}
