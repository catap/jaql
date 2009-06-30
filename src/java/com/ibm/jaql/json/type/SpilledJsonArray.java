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

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.JsonUtil;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.PagedFile;
import com.ibm.jaql.util.SpillFile;

/** A JSON array that stores its data in serialized form in a {@link SpillFile}. The array 
 * is append-only initially and read-only after it has been frozen; see {@link #freeze()}. 
 * 
 * The first elements of the array are cached in deserialized form in memory to improve 
 * efficiency for small arrays where spilling is not needed. 
 * 
 * When items are appended in serialized form and deserialization is not desired, the cache 
 * size can be set to zero.
 */
public class SpilledJsonArray extends JsonArray
{
  /** default cache size */
  public static final int DEFAULT_CACHE_SIZE = 256;

  /** number of elements stored in this array */
  protected long count = 0;
  
  /** paged file used to create spill file */
  protected PagedFile pagedFile;
  
  /** file used to store the content of the array in serialized form */
  protected SpillFile spillFile = null;

  /** Serializer used for writing to the spill file. At the moment, this *has* to be 
   * {@link DefaultBinaryFullSerializer}; that's why its static and final. Be careful when
   * changing this. */
  protected final static BinaryFullSerializer spillSerializer 
    = DefaultBinaryFullSerializer.getInstance(); // TODO: make dynamic
  
  /** number of elements to cache */
  protected int cacheSize;
  
  /** Cached version of the first <code>cacheSize</code> elements. The ith element of 
   * <code>cache</code> is the deserialized value of the ith element of this array. */
  protected JsonValue[] cache;
  
  /** flag indicating whether all values in the cache have been created by this spill array */
  protected boolean cacheIsMine;
  
  // utility variables
  private JsonValue tempValue = null; // do not make visible / return
  private final DataInputBuffer tempInputBuffer = new DataInputBuffer();
  
  // static variables 
  private static final JsonValue[] EMPTY_CACHE = new JsonValue[0];

  // -- constructors -----------------------------------------------------------------------------

  /** Creates a new <code>SpillJArray</code> that stores its data in the provided 
   * <code>file</code>. The first <code>cacheSize</code> elements are also cached in 
   * memory.
   * 
   * @param pagedFile a page file 
   * @param cacheSize number of elements to cache in memory
   * @param spillSerializer used to write values to the spill file
   */
  public SpilledJsonArray(PagedFile pagedFile, int cacheSize)
  {
    this.pagedFile = pagedFile;
    this.cacheSize = cacheSize;
    this.cache = cacheSize>0 ? new JsonValue[cacheSize] : EMPTY_CACHE; // TODO: grow incrementally?
    cacheIsMine = true;
    
  }

  /** Creates a new <code>SpillJArray</code> that stores its data in the provided 
   * <code>file</code>. The first {@link #DEFAULT_CACHE_SIZE} elements are
   * also cached in memory. 
   * 
   * @param pagedFile a page file 
   */
  public SpilledJsonArray(PagedFile pagedFile)
  {
    this(pagedFile, DEFAULT_CACHE_SIZE);
  }

  /** Creates an new <code>SpillJArray</code> using the default page file as determined by
   * {@link JaqlUtil#getQueryPageFile()}. The first <code>cacheSize</code> elements are 
   * also cached in memory.
   * 
   * @param cacheSize number of elements to cache in memory
   */
  public SpilledJsonArray(int cacheSize)
  {
    this(JaqlUtil.getQueryPageFile(), cacheSize);
  }

  /** Creates an new <code>SpillJArray</code> using the default page file as determined by
   * {@link JaqlUtil#getQueryPageFile()}. The first {@link #DEFAULT_CACHE_SIZE} elements are
   * also cached in memory. 
   */
  public SpilledJsonArray()
  {
    this(JaqlUtil.getQueryPageFile(), DEFAULT_CACHE_SIZE);
  }

  // -- caching ----------------------------------------------------------------------------------

  /** Sets the cache at index i to <code>value</code> without copying. */
  private void setCache(int i, JsonValue value) throws IOException {
    cache[i] = value;
  }

  /** Retrieves the ith value from the cache, if present. Otherwise, reads the value from
   * disk and caches it. 
   * 
   * @throws IOException */
  private JsonValue getCache(int i) throws IOException {
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
  
  public int getCacheSize()
  {
    return cacheSize;
  }
  
  public JsonValue[] getInternalCache() {
    return cache;
  }
  
  public BinaryFullSerializer getSpillSerializer() {
    return spillSerializer;
  }
  
  public void copySpillFile(DataOutput out) throws IOException {
    spillFile.writeToOutput(out); 
  }
  
  // -- business methods -------------------------------------------------------------------------
  
  /** Returns {@link JsonEncoding#ARRAY_SPILLING}.
   * 
   * @returns <code></code>Item.Encoding#ARRAY_SPILLING</code>
   * @see com.ibm.jaql.json.type.JsonValue#getEncoding() 
   */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.ARRAY_SPILLING;
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
  public JsonIterator iter() throws Exception
  {
    if (count == 0) {
      return JsonIterator.EMPTY;
    }
    freeze();
    
    return new JsonIterator() {
      int       i     = 0;
      DataInput input = null;
      boolean   eof   = false;

      public boolean moveNext() throws IOException {
        if (eof) {
          return false;
        }
        
        if (i<cacheSize) { // cache hit
          JsonValue internalValue = getCache(i);
          i++;
          eof = i>=count();
          currentValue = internalValue;
          return true;
        } else { // cache miss
          assert hasSpillFile();

          // get a copy of the input pointing to first non-cached item
          if (input == null) {
            input = spillFile.getInput();
            currentValue = null; // don't overwrite cached values
          }
       
          // read from input 
          currentValue = spillSerializer.read(input, currentValue);
          i++;
          eof = i>=count();
          return true;
        }
      }
    };
  }

  /** Returns the item at position <code>n</code> or <code>null<code> if there is no such 
   * element. This method will freeze the array if necessary.
   * 
   * @param n a position (0-based)
   * @return the item at position <code>n</code> or {@link JsonValue#NULL}
   * @throws Exception
   */
  @Override
  public JsonValue nth(long n) throws Exception 
  {
    if (n<0 || n>=count()) {
      return null;
    }
    if (n < cacheSize) {     // read from cache
      JsonValue value = getCache((int)n);
      JsonValue copiedValue = value.getEncoding().newInstance();
      copiedValue.setCopy(value);
      return copiedValue;
    } else {                  // read from file
      JsonIterator iter = iter();
      boolean valid = iter.moveN(n-cacheSize);
      assert valid;
      return iter.current();
    }
  }

  /** Copies the elements of this array into <code>values</code>. The length of <code>values</code>
   * has to be identical to the length of this array as produced by {@link #count()}.
   * This method will freeze the array if necessary.
   * 
   * @param items an array
   * @throws Exception
   */
  @Override
  public void getValues(JsonValue[] values) throws Exception // TODO: optimize
  {
    assert values.length == count();
    
    // init
    freeze();
    
    // fill up array
    int i = 0;
    for (JsonValue value : iter())
    {
      assert i<count();
      
      // set value
      values[i] = value;
      
      // next
      i++;
    }
  }

  //-- manipulation -----------------------------------------------------------------------------
  
  /** Copies all the elements provided by the specified iterator into this array, then freezes it. 
   * Clears the array before doing so.
   * 
   * @param iter an iterator
   * @throws Exception
   */
  public void setCopy(JsonIterator iter) throws Exception
  {
    clear();
    for (JsonValue value : iter) 
    {
      addCopy(value);
    }
    freeze();
  }

  /* @see com.ibm.jaql.json.type.JArray#copy(com.ibm.jaql.json.type.JValue) */
  @SuppressWarnings("static-access")
  @Override
  public void setCopy(JsonValue value) throws Exception
  {
    assert value != this;
    
    clear();
    SpilledJsonArray other = (SpilledJsonArray) value; 
    long newCount = other.count;

    // copy cache
    if (cacheSize != other.cacheSize) {
      cacheSize = other.cacheSize;
      cache = new JsonValue[cacheSize];
    }
    int m = newCount < cacheSize ? (int)newCount : cacheSize;
    for (int i=0; i<m; i++) {
      addCopy(other.cache[i]);
    }
    assert count == m;
    
    // copy spill file
    assert spillSerializer.equals(other.spillSerializer); // trivally true at the moment
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
    if (!cacheIsMine) {
      Arrays.fill(cache, null);
      cacheIsMine = true;
    }
    if (hasSpillFile()) {
      spillFile.clear();
    }    
    count = 0;
  }

  /** Appends either <code>value</code> or a copy thereof to this array. 
   * 
   * @param value a value
   * @return <code>true</code> if copied
   * @throws IOException
   */
  public boolean add(JsonValue value) throws IOException
  {
    boolean copied;
    if (count < cacheSize) {       // cache value
      setCache((int)count, value);
      cacheIsMine = cacheIsMine && value!=null;
      copied = false;
    } else {       // spill item
      ensureSpillFile();
      spillSerializer.write(spillFile, value);
      copied = true;
    }
    
    count++;
    return copied;
  }
  
  
  /** Appends a copy of <code>value</code> to this array.
   * 
   * @param value a value
   * @throws IOException
   */
  public void addCopy(JsonValue value) throws IOException
  {
    if (count < cacheSize) {      // cache value
      int i = (int)count;
      
      //  obtain a copy
      JsonValue copiedValue;
      try {
        if (value == null) {
          copiedValue = null;
        } else if (cacheIsMine) { // reuse instances
          copiedValue = value.getCopy(cache[i]);
        } else {
          copiedValue = value.getCopy(null);
        }
      } catch (Exception e)
      {
        throw new UnsupportedOperationException(e);
      }
      
      // and cache it
      setCache((int)count, copiedValue);
    } else { // spill item
      ensureSpillFile();
      spillSerializer.write(spillFile, value);
    }

    count++;
  }

  /** Appends a copy of an item given in its serialized form to this array.
   * 
   * @param input a n input stream
   * @param serializer serializer for input
   * @throws IOException 
   */
  public void addCopySerialized(DataInput input, BinaryFullSerializer serializer) throws IOException {
    if (count < cacheSize) {
      int i = (int)count;
      JsonValue value = null;
      if (cacheIsMine && cache[i] != null) {
        value = cache[i];
      }
      value = serializer.read(input, value);
      setCache(i, value);
    } else {
      ensureSpillFile();
      if (serializer.equals(spillSerializer)) {
    	ensureSpillFile();
        spillSerializer.copy(input, spillFile); 
      } else {
        tempValue = serializer.read(input, tempValue);
        spillSerializer.write(spillFile, tempValue);
        // value in tempValue is free to use
      }
    }

    count++;
  }
  
  /** Appends a copy of an item given in its serialized form to this array.
   * 
   * @param input a buffer
   * @param start offset at which the serialized item starts
   * @param length length of the serialized item
   * @param serializer serializer for input 
   * @throws IOException
   */
  public void addCopySerialized(byte[] input, int start, int length, BinaryFullSerializer serializer) 
  throws IOException
  {
    if (count < cacheSize || !serializer.equals(spillSerializer)) 
    {
      // deserialization required
      tempInputBuffer.reset(input, start, length);
      addCopySerialized(tempInputBuffer, serializer);
    } else {
      // copy directly, no need to deserialize
      ensureSpillFile();
      spillFile.write(input, start, length);
      count++;
    }    
  }
  
  /** Appends copies of all the items provided by <code>iter</code>.
   * 
   * @param iter an iterator  
   */
  public void addCopyAll(JsonIterator iter) throws Exception
  {
    for (JsonValue value : iter)
    {
      addCopy(value);
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
      spillFile.freeze();
    }
  }


  // -- comparison & hashing ----------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JArray#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    JsonArray t = (JsonArray) x;
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
