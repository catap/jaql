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

import com.ibm.jaql.json.util.ItemComparator;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.json.util.JIterator;
import com.ibm.jaql.json.util.JsonUtil;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.BaseUtil;
import com.ibm.jaql.util.PagedFile;
import com.ibm.jaql.util.SpillFile;

// TODO: Encoding.UNKOWN is currently used as a terminator for an array. This is not really
//       needed because the array length is stored up front (RG: is this true? see freeze!)

/** A JSON array that stores its data in serialized form in a {@link SpillFile}. The array 
 * is append-only initially and read-only after it has been frozen; see {@link #freeze()}. 
 * 
 * The first elements of the array are cached in deserialized form in memory to improve 
 * efficiency. The cache is filled when elements are appended to the array in deserialized
 * or on demand when elements are read. Thus, cache maintenance has neglibible overhead.
 */
public class SpillJArray extends JArray
{
  /** default cache size */
  public static final int DEFAULT_CACHE_SIZE = 256;

  /** number of elements stored in this array */
  protected long count;
  
  /** file used to store the content of the array in serialized form */
  protected SpillFile spillFile;

  /** number of elements to cache */
  protected int cacheSize;
  
  /** Cached version of the first <code>cacheSize</code> elements. The ith element of 
   * <code>cache</code> is the deserialized value of the ith element of this array
   * or <code>null</code> when the deserialized value is not known. */
  protected JValue[] cache;

  /** Input used to populating cache on demand and to avoid scanning cached items. */
  private SpillFile.SFDataInput cacheInput = null; // TODO: never closed 
  
  /** index of the element to which <code>cacheInput</code> is pointing */ 
  private int cacheInputPosition = -1;
  
  // utility variables
  private final Item internalTempItem = new Item(); // do not make visible
  private final Item externalTempItem = new Item(); // returned by some methods

  // static variables 
  private static final ItemComparator ITEM_COMPARATOR = new ItemComparator();
  private static final JValue[] EMPTY_CACHE = new JValue[0];

  // -- constructors -----------------------------------------------------------------------------
  
  /** Creates a new <code>SpillJArray</code> that stores its data in the provided 
   * <code>file</code>. The first <code>cacheSize</code> elements are also cached in 
   * memory.
   * 
   * @param pagedFile a page file 
   * @param cacheSize number of elements to cache in memory
   */
  public SpillJArray(PagedFile pagedFile, int cacheSize)
  {
    this.spillFile = new SpillFile(pagedFile);
    this.cacheSize = cacheSize;
    this.cache = cacheSize>0 ? new JValue[cacheSize] : EMPTY_CACHE; // TODO: grow incrementally?
  }
  
  /** Creates a new <code>SpillJArray</code> that stores its data in the provided 
   * <code>file</code>. The first {@link #DEFAULT_CACHE_SIZE} elements are
   * also cached in memory. 
   * 
   * @param pagedFile a page file 
   */
  public SpillJArray(PagedFile pagedFile)
  {
    this(pagedFile, DEFAULT_CACHE_SIZE);
  }

  /** Creates an new <code>SpillJArray</code> using the default page file as determined by
   * {@link JaqlUtil#getQueryPageFile()}. The first <code>cacheSize</code> elements are 
   * also cached in memory.
   * 
   * @param cacheSize number of elements to cache in memory
   */
  public SpillJArray(int cacheSize)
  {
    this(JaqlUtil.getQueryPageFile(), cacheSize);
  }

  /** Creates an new <code>SpillJArray</code> using the default page file as determined by
   * {@link JaqlUtil#getQueryPageFile()}. The first {@link #DEFAULT_CACHE_SIZE} elements are
   * also cached in memory. 
   */
  public SpillJArray()
  {
    this(JaqlUtil.getQueryPageFile(), DEFAULT_CACHE_SIZE);
  }

  // -- caching ----------------------------------------------------------------------------------

  /** Sets the cache at index i to value without copying. */
  private void setCache(int i, JValue value) throws IOException {
    assert i<cacheSize && i<=count;
    cache[i] = value;
  }

  /** Retrieves the ith values from the cache, if present. Otherwise, reads the value from
   * disk and caches it. 
   * 
   * @throws IOException */
  private JValue getCache(int i) throws IOException {
    assert i<cacheSize && i<count();
    
    if (cache[i] == null) { // not yet cached
      forwardCacheInputTo(i+1);
    }
    return cache[i]; // already cached
  }

  /** Forwards <code>cacheInput</code> to the specified position and caches all items read in
   * between (if not yet cached). The next item read from <code>cacheInput</code> will produce 
   * the array element at index <code>index</code>. The contract is that <code>cacheInput</code> 
   * is never reset and <code>index</code> is at most <code>cacheSize</code>.
   */
  private void forwardCacheInputTo(int index) throws IOException {
    assert cacheInputPosition <= index && index <= cacheSize && index <= count;

    // already there?
    if (cacheInputPosition == index) {
      return;
    }

    // initialize cache
    if (cacheInput == null) {
      cacheInput = spillFile.getInput();
      cacheInputPosition = 0;
    }    

    // move forward
    while (cacheInputPosition < index) {
      internalTempItem.readFields(cacheInput);
      assert internalTempItem.getEncoding() != Item.Encoding.UNKNOWN;
      
      if (cache[cacheInputPosition] == null) {
        cache[cacheInputPosition] = internalTempItem.get();
        internalTempItem.reset(); // don't keep reference to value
      }
      
      cacheInputPosition++;
    }
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
    if (count == 0) {
      return Iter.empty;
    }
    if (!spillFile.isFrozen())
    {
      freeze();
    }
    
    return new Iter() {
      int       i     = 0;
      DataInput input = null;
      Item      item  = new Item(); // TODO: memory
      boolean   eof   = false;

      public Item next() throws IOException {
        if (eof) {
          return null;
        }
        
        if (i<cacheSize) { // cache hit
          item.set(getCache(i));
          i++;
          eof = i>=count();
          return item;
        } else { // cache miss

          // get a copy of the input pointing to first non-cached item
          if (input == null) {
            item.reset(); // important to not overwrite internal cache with readFields()
            forwardCacheInputTo(cacheSize); // we known that cacheSize>=count()
            assert cacheInputPosition == i && i==cacheSize;            
            input = cacheInput.getCopy(); // points to first non-cached item
          }
       
          // read from input 
          item.readFields(input);
          if (item.getEncoding() != Item.Encoding.UNKNOWN)
          {
            i++;
            return item;
          }
          assert i==count();
          eof = true;
          return null;
        }
      }
    };
  }

  /** Returns the item at position <code>n</code> or nil if there is no such element. This method
   * will freeze the array if necessary.
   * 
   * @param n a position (0-based)
   * @return the item at position <code>n</code> or {@link Item#NIL}
   * @throws Exception
   */
  @Override
  public Item nth(long n) throws Exception 
  {
    if (n>=count()) {
      throw new ArrayIndexOutOfBoundsException();
    }
    if (n < cacheSize) {     // read from cache
      int i = (int)n;
      externalTempItem.set(getCache(i));
      return externalTempItem;
    } else {                  // read from file
      return iter().nth(n);
    }
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
    assert tuple.length == count();
    
    // init
    if (!spillFile.isFrozen())
    {
      freeze();
    }
    
    // fill up array
    Iter iter = iter();
    Item item = iter.next();
    int i = 0;
    while (item != null) {
      assert i<count();
      
      // copy item
      if (tuple[i] == null) tuple[i] = new Item();
      tuple[i].set(item.get());
      
      // next
      i++;
      item = iter.next();
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
      addCopy(item);
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
      addCopy(value);
    }
    freeze();
  }

  /* @see com.ibm.jaql.json.type.JArray#copy(com.ibm.jaql.json.type.JValue) */
  @Override
  public void setCopy(JValue value) throws Exception
  {
    clear();
    SpillJArray arr = (SpillJArray) value; 
    this.count = arr.count;
    this.spillFile.copy(arr.spillFile);
    // TODO: copy cache? 
  }
  
  /** Clears this array. New elements can be added using {@link #addCopy(Item)} or 
   * {@link #addCopySerialized(byte[], int, int)}. Before reading the data, {@link #freeze()} has to
   * be called.
   * 
   * @throws IOException
   */
  public void clear() throws IOException
  {
    spillFile.clear();
    cache = new JValue[cacheSize];
    cacheInput = null;
    cacheInputPosition = -1;
    count = 0;
  }

  /** Appends a copy of <code>item</code> to this array.
   * 
   * @param item an item
   * @throws IOException
   */
  public void addCopy(Item item) throws IOException
  {
    item.write(spillFile);
   
    // cache item
    if (count < cacheSize) {
      assert item != internalTempItem;
      
      try
      {
        internalTempItem.setCopy(item);
      } catch (Exception e)
      {
        throw new UnsupportedOperationException(e);
      }
      setCache((int)count, internalTempItem.get());
      internalTempItem.reset(); // don't keep reference to value
    }

    count++;
  }

  /** Wraps the provided <code>value</code> into an <code>Item</code> and appends it to this array.
   * 
   * @param value a value
   * @throws IOException
   */
  public void addCopy(JValue value) throws IOException
  {
    if (count < cacheSize) {
      // obtain a copy
      try {
        internalTempItem.setCopy(value);
      } catch (Exception e)
      {
        throw new UnsupportedOperationException(e);
      }
      
      // and cache it
      setCache((int)count, internalTempItem.get());
    } else { // do not cache
      internalTempItem.set(value);
    }

    count++;
    internalTempItem.write(spillFile);
    internalTempItem.reset(); // don't keep reference to value
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
    // copy directly to spill file; do not deserialize
    spillFile.write(val, off, len);
    count++;
  }
  
  /** Appends copies of all the items provided by <code>iter</code>.
   * 
   * @param iter an iterator  
   */
  public void addCopyAll(Iter iter) throws Exception
  {
    Item item;
    while( (item = iter.next()) != null )
    {
      addCopy(item);
    }
  }


  /** Freezes this array. This function should be called after the array has been constructed and
   * before it is read. After calling this function, no further adds but can be performed. However,
   * it is OK to {@link #clear()} the array or call any of the modification methods that clear 
   * the array before performing the modification (such as the <code>setCopy</code> methods or 
   * {@link #readFields(DataInput)}. 
   * 
   * @throws IOException
   */
  public void freeze() throws IOException
  {
    BaseUtil.writeVUInt(spillFile, Item.Encoding.UNKNOWN.id); // marks eof
    spillFile.freeze();
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
    clear();
    count = BaseUtil.readVULong(in);
    long len = BaseUtil.readVULong(in);
    spillFile.writeFromInput(in, len);
    // terminator is copied from input
    spillFile.freeze();
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
    if (!spillFile.isFrozen())
    {
      freeze();
    }
    // Be sure to update ItemWalker whenever changing this.
    BaseUtil.writeVULong(out, count);
    BaseUtil.writeVULong(out, spillFile.size()); // TODO: make blocked?
    spillFile.writeToOutput(out);
  }


  /**
   * @return
   */
  public SpillFile getSpillFile()
  {
    return spillFile;
  }

  
  // -- comparison & hashing ----------------------------------------------------------------------
  
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
        if (!spillFile.isFrozen())
        {
          freeze();
        }
        if (!st.spillFile.isFrozen())
        {
          st.freeze();
        }
        synchronized (ITEM_COMPARATOR) // TODO: SMP problem here eventually...
        {
          // TODO: does not exploit cache
          return ITEM_COMPARATOR.compareSpillArrays(spillFile.getInput(), st.spillFile
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
