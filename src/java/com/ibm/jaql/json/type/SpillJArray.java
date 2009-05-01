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
import java.util.Arrays;

import org.apache.hadoop.io.DataInputBuffer;

import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.json.util.JIterator;
import com.ibm.jaql.json.util.JsonUtil;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.BaseUtil;
import com.ibm.jaql.util.PagedFile;
import com.ibm.jaql.util.SpillFile;

// TODO: Encoding.UNKOWN is currently used as a terminator for an array. This is not really
//       needed because the array length is stored up front.

/** A JSON array that stores its data in serialized form in a {@link SpillFile}. The array 
 * is append-only initially and read-only after it has been frozen; see {@link #freeze()}. 
 * 
 * The first elements of the array are cached in deserialized form in memory to improve 
 * efficiency for small arrays where spilling is not needed. 
 * 
 * When items are appended in serialized form and deserialization is not desired, the cache 
 * size can be set to zero.
 */
public class SpillJArray extends JArray
{
  /** default cache size */
  public static final int DEFAULT_CACHE_SIZE = 256;

  /** number of elements stored in this array */
  protected long count = 0;
  
  /** paged file used to create spill file */
  protected PagedFile pagedFile;
  
  /** file used to store the content of the array in serialized form */
  protected SpillFile spillFile = null;

  /** number of elements to cache */
  protected int cacheSize;
  
  /** Cached version of the first <code>cacheSize</code> elements. The ith element of 
   * <code>cache</code> is the deserialized value of the ith element of this array. */
  protected JValue[] cache;

  // utility variables
  private final Item internalTempItem = new Item(); // do not make visible
  private final Item externalTempItem = new Item(); // returned by some methods
  private final DataInputBuffer tempInputBuffer = new DataInputBuffer();
  
  // static variables 
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
    this.pagedFile = pagedFile;
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

  /** Sets the cache at index i to <code>value</code> without copying. */
  private void setCache(int i, JValue value) throws IOException {
    cache[i] = value;
  }

  /** Retrieves the ith value from the cache, if present. Otherwise, reads the value from
   * disk and caches it. 
   * 
   * @throws IOException */
  private JValue getCache(int i) throws IOException {
    return cache[i]; // already cached
  }

  /** Makes sure that there is a spill file */
  private void ensureSpillFile() {
    if (spillFile == null) {
      spillFile = new SpillFile(pagedFile);
    }
  }
  
  public boolean hasSpillFile() {
    return spillFile != null;
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
    freeze();
    
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
          assert hasSpillFile();

          // get a copy of the input pointing to first non-cached item
          if (input == null) {
            item.reset(); // important to not overwrite internal cache with readFields()
            input = spillFile.getInput();
          }
       
          // read from input 
          item.readFields(input);
          if (item.getEncoding() != Item.Encoding.UNKNOWN)
          {
            // i not really needed anymore
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
      externalTempItem.set(getCache((int)n));
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
    freeze();
    
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
    assert value != this;
    
    clear();
    SpillJArray other = (SpillJArray) value; 
    long newCount = other.count;

    // copy cache
    if (cacheSize != other.cacheSize) {
      cacheSize = other.cacheSize;
      cache = new JValue[cacheSize];
    }
    int m = newCount < cacheSize ? (int)newCount : cacheSize;
    for (int i=0; i<m; i++) {
      addCopy(other.cache[i]);
    }
    assert count == m;
    
    // copy spill file
    if (other.spillFile != null) {
      ensureSpillFile();
      spillFile.copy(other.spillFile);
      count = newCount;
    }
    
    assert count == newCount;
  }
  
  /** Clears this array. New elements can be added using {@link #addCopy(Item)} or 
   * {@link #addCopySerialized(byte[], int, int)}. Before reading the data, {@link #freeze()} has to
   * be called.
   * 
   * @throws IOException
   */
  public void clear() throws IOException
  {
    count = 0;
    Arrays.fill(cache, null);
    if (hasSpillFile()) {
      spillFile.clear();
    }    
  }

  public void add(Item item) throws IOException
  {
    if (count < cacheSize) {       // cache item
      setCache((int)count, item.get());
    } else {       // spill item
      ensureSpillFile();
      item.write(spillFile);
    }
    
    count++;
  }
  
  public void add(JValue v) throws IOException {
    internalTempItem.set(v);
    add(internalTempItem);
    internalTempItem.reset();
  }
  
  /** Appends a copy of the <code>item</code>'s value to this array.
   * 
   * @param item an item
   * @throws IOException
   */
  public void addCopy(Item item) throws IOException
  {
    if (count < cacheSize) {      // cache item
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
    } else {    // spill item
      ensureSpillFile();
      item.write(spillFile);
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
    if (count < cacheSize) {      // cache item
      // obtain a copy
      try {
        internalTempItem.setCopy(value);
      } catch (Exception e)
      {
        throw new UnsupportedOperationException(e);
      }
      
      // and cache it
      setCache((int)count, internalTempItem.get());
    } else { // spill item
      ensureSpillFile();
      internalTempItem.set(value);
      internalTempItem.write(spillFile);      
    }

    count++;
    internalTempItem.reset(); // don't keep reference to value
  }

  /** Appends a copy of an item given in its serialized form to this array.
   * 
   * @param input a buffer
   * @param start offset at which the serialized item starts
   * @param length length of the serialized item 
   * @throws IOException
   */
  public void addCopySerialized(byte[] input, int start, int length) throws IOException
  {
    if (count < cacheSize) {
      // deserialize
      tempInputBuffer.reset(input, start, length);
      internalTempItem.readFields(tempInputBuffer);
      add(internalTempItem); // no copy needed
      internalTempItem.reset();
    } else {
      // copy directly to spill file; do not deserialize
      spillFile.write(input, start, length);
    }

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
    if (hasSpillFile() && !spillFile.isFrozen()) {
      BaseUtil.writeVUInt(spillFile, Item.Encoding.UNKNOWN.id); // marks eof
      spillFile.freeze();
    }
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
    count = 0;
    long newCount = BaseUtil.readVULong(in);
//    long len = BaseUtil.readVULong(in);
    
    // read into cache
    int m = newCount < cacheSize ? (int)newCount  : cacheSize;
    for (int i=0; i<m; i++) {
      internalTempItem.readFields(in);
      add(internalTempItem); // no copy needed
      internalTempItem.reset();
    }
    
    // read into spill file
    if (newCount > cacheSize) {    
      // TODO: very inefficient, need copySerialized() method on all JValues
      ensureSpillFile();
      for (int i=m; i<newCount; i++) {
        internalTempItem.readFields(in);
        addCopy(internalTempItem); // it's actually not copied: will get serialized
      }
    }
    assert count==newCount;
    
    // terminator
    int terminator = BaseUtil.readVUInt(in);
    assert terminator == Item.Encoding.UNKNOWN.id;
    freeze();
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
    freeze();
    // Be sure to update ItemWalker whenever changing this.
    BaseUtil.writeVULong(out, count);
//    if (hasSpillFile()) {
//      BaseUtil.writeVULong(out, spillFile.size()); // TODO: make blocked?
//    } else {
//      BaseUtil.writeVULong(out, 0); // TODO: make blocked?
//    }
    
    // write cached items
    int m = count < cacheSize ? (int)count : cacheSize;
    for (int i=0; i<m; i++) {
      internalTempItem.set(getCache(i));
      internalTempItem.write(out);
    }
    internalTempItem.reset();
    
    // write spilled items
    if (hasSpillFile()) {
      spillFile.writeToOutput(out); // copies terminator
    } else {
      // write terminator
      BaseUtil.writeVUInt(out, Item.Encoding.UNKNOWN.id);
    }
  }


  // -- comparison & hashing ----------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JArray#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    JArray t = (JArray) x;
    // TODO: avoid deserialization when x is also a spillarray (requires binary item comparison)
    try
    {
      return JsonUtil.deepCompare(iter(), t.iter());
    } catch (Exception e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

}
