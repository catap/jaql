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

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import com.ibm.jaql.io.serialization.def.DefaultFullSerializer;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.util.BaseUtil;
import com.ibm.jaql.util.LongArray;

/**
 * 
 */
public class ItemHashtable
{
  /**
   * 
   */
  protected static class Entry
  {
    int         hashCode;
    long        keyOffset;
    long        keyLength;
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
  Item             key2   = new Item();
  DataOutputBuffer outbuf = new DataOutputBuffer();
  DataInputBuffer  inbuf  = new DataInputBuffer();
  int              numTags;
  long             numKeys;
  long             numValues;

  /**
   * @param numTags
   */
  public ItemHashtable(int numTags)
  {
    this.numTags = numTags;
  }

  public long numKeys()
  {
    return numKeys;
  }
  
  public long numValues()
  {
    return numValues;
  }
  
  public long getMemoryUsage()
  {
    return outbuf.size() + numKeys * (5*8) + numValues * 8; // TODO: why is size() an int??
  }
  
  public void reset()
  {
    for(int i = 0 ; i < table.length ; i++)
    {
      table[i] = null;
    }
    outbuf.reset();
    numKeys = 0;
    numValues = 0;
  }
  
  /**
   * This must be true: 0 <= tag < numTags
   * 
   * @param tag
   * @param key
   * @param value
   * @throws IOException
   */
  public void add(int tag, Item key, Item value) throws IOException
  {
    int h = key.hashCode();
    int i = (h & 0x7fffffff) % table.length; // TODO: use fast mod (pow 2?)
    Entry e;
    for (e = table[i]; e != null; e = e.next)
    {
      if (e.hashCode == h)
      {
        inbuf.reset(outbuf.getData(), (int) e.keyOffset, outbuf.getLength());
        key2.readFields(inbuf);
        if (key2.equals(key)) // TODO: use RawComparator or keep deserialized
        {
          break;
        }
      }
    }

    if (e == null)
    {
      numKeys++;
      e = new Entry(numTags);
      e.hashCode = h;
      e.next = table[i];
      table[i] = e;
      e.keyOffset = outbuf.getLength();
      key.write(outbuf);
      e.keyLength = outbuf.getLength() - e.keyOffset;
    }
    numValues++;
    long offset = outbuf.getLength();
    e.values[tag].add(offset);
    value.write(outbuf);
    e.values[tag].add(outbuf.getLength() - offset);
  }

  public void write(DataOutput out, int tag) throws IOException
  {
    //   this needs to write the keys on one chain in a well-defined order so we can merge them!
    

    for( Entry e1: table )
    {
      for( Entry e = e1 ; e != null ; e = e.next )
      {
        out.write(outbuf.getData(), (int)e.keyOffset, (int)e.keyLength);
        LongArray la = e.values[tag];
        int n = la.size();
        BaseUtil.writeVUInt(out, n / 2);
        for(int i = 0 ; i < n ; i += 2)
        {
          out.write(outbuf.getData(), (int)la.get(i), (int)la.get(i+1));
        }
      }
    }
  }

  /**
   * 
   */
  public class Iterator
  {
    protected int         index = 0;
    protected Entry       entry = null;
    protected SpillJArray valuesArray[];
    protected Item        vitem = new Item();
    protected Item        key   = new Item();
    protected Item        values[];

    /**
     * 
     */
    public Iterator()
    {
      valuesArray = new SpillJArray[numTags];
      values = new Item[numTags];
      for (int i = 0; i < numTags; i++)
      {
        valuesArray[i] = new SpillJArray();
        values[i] = new Item(valuesArray[i]);
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

      inbuf.reset(outbuf.getData(), (int) entry.keyOffset, (int) entry.keyLength);
      key.readFields(inbuf);

      // It's sad to copy all the values... I could create a new Table type, but it has to be JaqlType.
      for (int i = 0; i < numTags; i++)
      {
        SpillJArray va = valuesArray[i];
        va.clear();
        LongArray voffsets = entry.values[i];
        for (int j = 0; j < voffsets.size(); j += 2)
        {
          va.addCopySerialized(outbuf.getData(), (int) voffsets.get(j), (int) voffsets.get(j+1),
              DefaultFullSerializer.getDefault());
        }
        va.freeze();
      }

      return true;
    }

    /**
     * @return
     */
    public Item key()
    {
      return key;
    }

    /**
     * @param tag
     * @return
     */
    public Item values(int tag)
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
