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
package com.ibm.jaql.json.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.json.util.DataInputTableIter;
import com.ibm.jaql.json.util.ItemComparator;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.json.util.JIterator;
import com.ibm.jaql.json.util.JsonUtil;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.BaseUtil;
import com.ibm.jaql.util.PagedFile;
import com.ibm.jaql.util.SpillFile;

/** A JSON array that stores its data in a {@link SpillFile}. */
public class SpillJArray extends JArray
{
  protected long      count;
  protected SpillFile spill;
  protected Item      tempItem = new Item();

  /**
   * @param file TODO
   * 
   */
  public SpillJArray(PagedFile file)
  {
    spill = new SpillFile(file);
  }

  /**
   * 
   */
  public SpillJArray()
  {
    this(JaqlUtil.getQueryPageFile());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.ARRAY_SPILLING;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#count()
   */
  @Override
  public final long count()
  {
    return count;
  }

  /**
   * @param iter
   * @throws Exception
   */
  public void set(Iter iter) throws Exception
  {
    spill.clear();
    count = 0;
    Item item;
    while ((item = iter.next()) != null)
    {
      item.write(spill);
      count++;
    }
    freeze();
  }

  /**
   * @param iter
   * @throws Exception
   */
  public void set(JIterator iter) throws Exception
  {
    spill.clear();
    count = 0;
    while (iter.moveNext())
    {
      JValue value = iter.current();
      tempItem.set(value); // FIXME: tempItem doesn't own this value!! (bug when we start Item's caching)
      tempItem.write(spill);
      count++;
    }
    freeze();
  }

  /**
   * Follow this will add() or addSerialized() calls, then freeze(). Freeze must
   * be called before reading the array
   * 
   * @throws IOException
   */
  public void clear() throws IOException
  {
    spill.clear();
    count = 0;
  }

  /**
   * Be sure to call freeze after using this method
   * 
   * @param item
   * @throws IOException
   */
  public void add(Item item) throws IOException
  {
    item.write(spill);
    count++;
  }

  /**
   * Be sure to call freeze after using this method
   * 
   * @param value
   * @throws IOException
   */
  public void add(JValue value) throws IOException
  {
    tempItem.set(value);
    add(tempItem);
  }

  /**
   * Be sure to call freeze after using this method
   * 
   * @param val
   * @param off
   * @param len
   * @throws IOException
   */
  public void addSerialized(byte[] val, int off, int len) throws IOException
  {
    // copy directly to buffer
    spill.write(val, off, len);
    count++;
  }

  /**
   * @throws IOException
   */
  public void freeze() throws IOException
  {
    BaseUtil.writeVUInt(spill, Item.Encoding.UNKNOWN.id); // marks eof
    spill.freeze();
  }

  /*
   * This will freeze if necessary (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#iter()
   */
  @Override
  public Iter iter() throws Exception
  {
    if (count == 0)
    {
      return Iter.empty;
    }
    if (!spill.isFrozen())
    {
      freeze();
    }
    SpillFile.SFDataInput input = spill.getInput(); // TODO: cache
    return new DataInputTableIter(input); // TODO: cache
  }

  /*
   * This will freeze if necessary (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#nth(long)
   */
  @Override
  public Item nth(long n) throws Exception // TODO: optimize
  {
    Iter iter = this.iter();
    return iter.nth(n);
  }

  /*
   * This clears and reloads the array (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    spill.clear();
    count = BaseUtil.readVULong(in);
    long len = BaseUtil.readVULong(in);
    spill.writeFromInput(in, len);
    // terminator is copied from input
    spill.freeze();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    if (!spill.isFrozen())
    {
      freeze();
    }
    // Be sure to update ItemWalker whenever changing this.
    BaseUtil.writeVULong(out, count);
    BaseUtil.writeVULong(out, spill.size()); // TODO: make blocked?
    spill.writeToOutput(out);
  }

  private static ItemComparator itemComparator = new ItemComparator();

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object x)
  {
    JArray t = (JArray) x;
    try
    {
      if (t instanceof SpillJArray)
      {
        SpillJArray st = (SpillJArray) t;
        if (!spill.isFrozen())
        {
          freeze();
        }
        if (!st.spill.isFrozen())
        {
          st.freeze();
        }
        synchronized (itemComparator) // TODO: SMP problem here eventually...
        {
          return itemComparator.compareSpillArrays(spill.getInput(), st.spill
              .getInput());
        }
      }
      return JsonUtil.deepCompare(iter(), t.iter());
    }
    catch (Exception ex)
    {
      throw new UndeclaredThrowableException(ex);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#getTuple(com.ibm.jaql.json.type.Item[])
   */
  @Override
  public void getTuple(Item[] tuple) throws Exception // TODO: optimize
  {
    if (!spill.isFrozen())
    {
      freeze();
    }
    DataInput input = spill.getInput();
    for (int i = 0; i < tuple.length; i++)
    {
      if (tuple[i] == null)
      {
        tuple[i] = new Item();
      }
      tuple[i].readFields(input);
    }
    if (BaseUtil.readVUInt(input) != Item.Encoding.UNKNOWN.id)
    {
      throw new RuntimeException("expected exactly " + tuple.length
          + " but found more");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    // TODO: store hash code while creating the array?
    try
    {
      if (!spill.isFrozen())
      {
        freeze();
      }
      DataInput input = spill.getInput();
      Item item = new Item(); // TODO: memory

      long h = initHash();
      while (true)
      {
        item.readFields(input);
        if (item.getEncoding() == Item.Encoding.UNKNOWN)
        {
          return h;
        }
        h = hashItem(h, item);
      }
    }
    catch (IOException ex)
    {
      throw new UndeclaredThrowableException(ex);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JValue value) throws Exception
  {
    SpillJArray arr = (SpillJArray) value;
    this.count = arr.count;
    this.spill.copy(arr.spill);
  }

  /**
   * @return
   */
  public SpillFile getSpillFile()
  {
    return spill;
  }

  public void addAll(Iter iter) throws Exception
  {
    Item item;
    while( (item = iter.next()) != null )
    {
      add(item);
    }
  }
}
