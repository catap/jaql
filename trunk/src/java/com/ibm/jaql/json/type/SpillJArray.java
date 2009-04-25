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

// TODO: Encoding.UNKOWN is currently used as a terminator for an array. This is not really
//       needed because the array length is stored up front

/** A JSON array that stores its data in a {@link SpillFile}. */
public class SpillJArray extends JArray
{
  protected long      count;
  protected SpillFile spill;
  protected Item      tempItem = new Item();

  private static ItemComparator ITEM_COMPARATOR = new ItemComparator();
  
  // -- constructors -----------------------------------------------------------------------------
  
  /** Creates a new <code>SpillJArray</code> that stores its data in the provided 
   * <code>file</code>.
   * 
   * @param file a page file 
   */
  public SpillJArray(PagedFile file)
  {
    spill = new SpillFile(file);
  }

  /** Creates an new <code>SpillJArray</code> using the default page file as determined by
   * {@link JaqlUtil#getQueryPageFile()}. 
   */
  public SpillJArray()
  {
    this(JaqlUtil.getQueryPageFile());
  }

  
  // -- business methods -------------------------------------------------------------------------
  
  /** Returns {@link Item.Encoding#ARRAY_SPILLING}.
   * 
   * @returns <code></code>Item.Encoding#ARRAY_SPILLING</code>
   * @see com.ibm.jaql.json.type.JValue#getEncoding() 
   */
  @Override
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.ARRAY_SPILLING;
  }

  /* @see com.ibm.jaql.json.type.JArray#count() */
  @Override
  public final long count()
  {
    return count;
  }

  /** Returns an <code>Iter</code> over the elements in this array. This method will freeze the
   * array if necessary.
   *   
   * @return an <code>Iter</code> over the elements in this array
   * @throws Exception
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

  /** Returns the item at position <code>n</code> or nil if there is no such element. This method
   * will freeze the array if necessary.
   * 
   * @param n a position (0-based)
   * @return the item at position <code>n</code> or {@link Item#nil}
   * @throws Exception
   */
  @Override
  public Item nth(long n) throws Exception 
  {
    // TODO: optimize
    Iter iter = this.iter();
    return iter.nth(n);
  }

  /** Copies the elements of this array into <code>items</code>. The length of <code>items</code>
   * has to be identical to the length of this array as produced by {@link #count()}. This method
   * will freeze the array if necessary.
   * 
   * @param items an array
   * @throws Exception
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

  //-- manipulation -----------------------------------------------------------------------------
  
  /** Copies all the elements provided by the specified iterator into this array, then freezes it. 
   * Clears the array before doing so.
   * 
   * @param iter an iterator
   * @throws Exception
   */
  public void setCopy(Iter iter) throws Exception
  {
    clear();
    Item item;
    while ((item = iter.next()) != null)
    {
      item.write(spill);
      count++;
    }
    freeze();
  }

  /** Copies all the elements provided by the specified iterator into this array, then freezes it. 
   * Clears the array before doing so.
   * 
   * @param iter an iterator
   * @throws Exception
   */
  public void setCopy(JIterator iter) throws Exception
  {
    clear();
    while (iter.moveNext())
    {
      JValue value = iter.current();
      tempItem.set(value); // FIXME: tempItem doesn't own this value!! (bug when we start Item's caching)
      tempItem.write(spill);
      count++;
    }
    freeze();
  }

  /* @see com.ibm.jaql.json.type.JArray#copy(com.ibm.jaql.json.type.JValue) */
  @Override
  public void setCopy(JValue value) throws Exception
  {
    SpillJArray arr = (SpillJArray) value;
    this.count = arr.count;
    this.spill.copy(arr.spill);
  }
  
  /** Clears this array. New elements can be added using {@link #addCopy(Item)} or 
   * {@link #addCopySerialized(byte[], int, int)}. Before reading the data, {@link #freeze()} has to
   * be called.
   * 
   * @throws IOException
   */
  public void clear() throws IOException
  {
    spill.clear();
    count = 0;
  }

  /** Appends a copy of <code>item</code> to this array.
   * 
   * @param item an item
   * @throws IOException
   */
  public void addCopy(Item item) throws IOException
  {
    item.write(spill);
    count++;
  }

  /** Wraps the provided <code>value</code> into an <code>Item</code> and appends it to this array.
   * 
   * @param value a value
   * @throws IOException
   */
  public void addCopy(JValue value) throws IOException
  {
    tempItem.set(value);
    addCopy(tempItem);
  }

  /** Appends a copy of an item given in its serialized form to this array.
   * 
   * @param val a buffer
   * @param off offset at which the serialized item starts
   * @param len length of the serialized item 
   * @throws IOException
   */
  public void addCopySerialized(byte[] val, int off, int len) throws IOException
  {
    // copy directly to buffer
    spill.write(val, off, len);
    count++;
  }
  
  /** Appends copies of all the items provided by <code>iter</code>.
   * 
   * @param iter an iterator  
   */
  public void addAllCopies(Iter iter) throws Exception
  {
    Item item;
    while( (item = iter.next()) != null )
    {
      addCopy(item);
    }
  }


  /** Freezes this array. This function should be called after the array has been constructed and
   * before it is used. After calling this function, no further adds but can be performed. However,
   * it is OK to {@link #clear()} the array or call any of the modification methods that clear 
   * the array before performing the modification (such as the <code>setCopy</code> methods or 
   * {@link #readFields(DataInput)}. 
   * 
   * @throws IOException
   */
  public void freeze() throws IOException
  {
    BaseUtil.writeVUInt(spill, Item.Encoding.UNKNOWN.id); // marks eof
    spill.freeze();
  }

  
  /** Read a serialized array from the provided input. Clears this array before doing so.
   * 
   * @param in an input stream
   * @throws IOException
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataOutput)
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

  /** Freezes the array and writes it to the provided output. 
   * 
   * @param out an output stream
   * @throws IOException
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


  /**
   * @return
   */
  public SpillFile getSpillFile()
  {
    return spill;
  }

  
  // -- comparison & hashing ----------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JArray#hashCode() */
  @Override
  public int hashCode()
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

      int h = initHash();
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

  /* @see com.ibm.jaql.json.type.JArray#longHashCode() */
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

      long h = initLongHash();
      while (true)
      {
        item.readFields(input);
        if (item.getEncoding() == Item.Encoding.UNKNOWN)
        {
          return h;
        }
        h = longHashItem(h, item);
      }
    }
    catch (IOException ex)
    {
      throw new UndeclaredThrowableException(ex);
    }
  }

  /* @see com.ibm.jaql.json.type.JArray#compareTo(java.lang.Object) */
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
        synchronized (ITEM_COMPARATOR) // TODO: SMP problem here eventually...
        {
          return ITEM_COMPARATOR.compareSpillArrays(spill.getInput(), st.spill
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

}
