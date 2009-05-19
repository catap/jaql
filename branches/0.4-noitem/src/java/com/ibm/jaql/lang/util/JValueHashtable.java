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
package com.ibm.jaql.lang.util;

import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.util.LongArray;

/**
 * 
 */
public class JValueHashtable
{
  BinaryFullSerializer serializer = BinaryFullSerializer.getDefault();
  
  /**
   * 
   */
  protected static class Entry
  {
    int         hashCode;
    long        keyOffset;
    LongArray[] values;   // offset of each value with this key, partitioned by tag
    Entry       next;

    /**
     * @param numTags
     */
    public Entry(int numTags)
    {
      values = new LongArray[numTags];
      for (int i = 0; i < values.length; i++)
      {
        values[i] = new LongArray();
      }
    }
  }

  Entry[]          table  = new Entry[1000];       // TODO: make dynamic
  JsonValue           key2   = null;
  DataOutputBuffer outbuf = new DataOutputBuffer();
  DataInputBuffer  inbuf  = new DataInputBuffer();
  int              numTags;

  /**
   * @param numTags
   */
  public JValueHashtable(int numTags)
  {
    this.numTags = numTags;
  }

  /**
   * This must be true: 0 <= tag < numTags
   * 
   * @param tag
   * @param key
   * @param value
   * @throws IOException
   */
  public void add(int tag, JsonValue key, JsonValue value) throws IOException
  {
    int h = key.hashCode();
    int i = (h & 0x7fffffff) % table.length; // TODO: use fast mod (pow 2?)
    Entry e;
    for (e = table[i]; e != null; e = e.next)
    {
      if (e.hashCode == h)
      {
        inbuf.reset(outbuf.getData(), (int) e.keyOffset, outbuf.getLength());
        key2 = serializer.read(inbuf, key2);
        if (key2.equals(key))
        {
          break;
        }
      }
    }

    if (e == null)
    {
      e = new Entry(numTags);
      e.hashCode = h;
      e.next = table[i];
      table[i] = e;
      e.keyOffset = outbuf.getLength();
      serializer.write(outbuf, key);
    }
    e.values[tag].add(outbuf.getLength());
    serializer.write(outbuf, value);
  }

  /**
   * 
   */
  public class Iterator
  {
    protected int         index = 0;
    protected Entry       entry = null;
    protected SpilledJsonArray values[];
    protected JsonValue      value = null;
    protected JsonValue      key   = null;

    /**
     * 
     */
    public Iterator()
    {
      values = new SpilledJsonArray[numTags];
      for (int i = 0; i < numTags; i++)
      {
        values[i] = new SpilledJsonArray();
      }
    }

    /**
     * @return
     * @throws IOException
     */
    public boolean next() throws IOException
    {
      if (entry != null)
      {
        entry = entry.next;
      }
      while (entry == null)
      {
        if (index == table.length)
        {
          return false;
        }
        entry = table[index];
        index++;
      }

      inbuf.reset(outbuf.getData(), (int) entry.keyOffset, outbuf.getLength());
      key = serializer.read(inbuf, key);

      // It's sad to copy all the values... I could create a new Table type, but it has to be JaqlType.
      for (int i = 0; i < numTags; i++)
      {
        SpilledJsonArray va = values[i];
        va.clear();
        LongArray voffsets = entry.values[i];
        for (int j = 0; j < voffsets.size(); j++)
        {
          inbuf.reset(outbuf.getData(), (int) voffsets.get(j), outbuf
              .getLength());
          value = serializer.read(inbuf, value);
          boolean copied = va.add(value);
          if (!copied) value = null;
        }
        va.freeze();
      }

      return true;
    }

    /**
     * @return
     */
    public JsonValue key()
    {
      return key;
    }

    /**
     * @param tag
     * @return
     */
    public JsonValue values(int tag)
    {
      return values[tag];
    }
  }

  /**
   * @return
   */
  public Iterator iter()
  {
    return new Iterator();
  }
}
