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

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


/** An in-memory {@link JsonRecord}. Name-based field access is implemented using a hash table.
 * In addition, this class provides functionality to access record fields by index. This is 
 * possible because the main data structure used by this record is a parallel array: one array 
 * storing the field names and one array storing the corresponding field values. As long as the 
 * record names are not modified, field indexes are stable and can be used to avoid the look-up 
 * cost of name-based accesses.
 * 
 * Quick record construction is supported via the {@link #setInternal(JsonString[], JsonValue[], 
 * int, boolean)} method.  
 */
public class BufferedJsonRecord extends JsonRecord {
  protected static final int MIN_CAPACITY = 8; // when non-empty
  protected static final JsonString[] NOTHING = new JsonString[0];
  
  // names and values are parallel arrays with names being kept in arbitrary order
  // invariants: size<=names.length==values.length
  protected JsonString[]     names  = NOTHING;			 
  protected JsonValue[]      values = NOTHING;		   
  protected int              size   = 0;

  // index structures (we could be more efficent by implementing our own hash maps) 
  protected Map<JsonString, Integer> hashIndex; 
  boolean isSorted = true; // empty record is always sorted

  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs an empty in-memory JSON record */
  public BufferedJsonRecord()
  {
    hashIndex = new HashMap<JsonString, Integer>();
  }

  /** Constructs an empty in-memory JSON record and allocates space for the specified number of
   * entries */
  public BufferedJsonRecord(int capacity)
  {
    names = new JsonString[capacity];
    values = new JsonValue[capacity];
    hashIndex = new HashMap<JsonString, Integer>(capacity);
  }

  // -- reading -----------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonRecord#size() */
  @Override
  public int size()
  {
    return size;
  }

  /* @see com.ibm.jaql.json.type.JsonRecord#get(com.ibm.jaql.json.type.JsonString,
   *      com.ibm.jaql.json.type.JsonValue) */
  @Override
  public JsonValue get(JsonString name, JsonValue defaultValue)
  {
    int index = indexOf(name);
    return index >= 0 ? values[index] : defaultValue;
  }

  /* @see com.ibm.jaql.json.type.JsonRecord#get(com.ibm.jaql.json.type.JsonString) */
  @Override
  public JsonValue getRequired(JsonString key)
  {
    int index = indexOf(key);
    if (index < 0) throw new IllegalArgumentException("invalid field name " + key);
    return values[index];
  }
  
  /* @see com.ibm.jaql.json.type.JsonRecord#containsKey(com.ibm.jaql.json.type.JsonString) */
  @Override
  public boolean containsKey(JsonString key)
  {
    return indexOf(key) >= 0;
  }
  
  /* @see com.ibm.jaql.json.type.JsonRecord#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public BufferedJsonRecord getCopy(JsonValue target) throws Exception {
    return getCopy(target, true);
  }
  
  /** Returns a shallow copy of this record. They key data strutures are copied, but the actual
   * field names and field values are not. */
  public BufferedJsonRecord getShallowCopy(JsonValue target) throws Exception {
    return getCopy(target, false);
  }
  
  protected BufferedJsonRecord getCopy(JsonValue target, boolean deep) throws Exception {
    if (target == this) target = null;
    
    // determine target record
    BufferedJsonRecord t;
    if (target instanceof BufferedJsonRecord)
    {
      t = (BufferedJsonRecord)target;
    }
    else
    {
      t = new BufferedJsonRecord(this.size);
    }
    t.resize(this.size);
    
    // copy data
    for (int i = 0; i < size; i++)
    {
      t.names[i] = deep ? JsonUtil.getCopy(names[i], t.names[i]) : names[i]; 
      t.values[i] = deep ? JsonUtil.getCopy(values[i], t.values[i]) : values[i];
    }
    t.isSorted = isSorted;
    if (deep)
    {
      t.reindex();
    }
    else
    {
      t.hashIndex.clear();
      t.hashIndex.putAll(hashIndex);
    }
    
    return t;
  } 

  
  // -- index-based access ------------------------------------------------------------------------

  /** Searches this record for the specified field and returns its index. Returns a negative
   * number when the field name has not been found. */
  public int indexOf(JsonString name)
  {
  	Integer index = hashIndex.get(name);
  	return index==null ? -1 : index;
  }

  /** Returns the field located at the specified index without boundary checking. This
   * method suceeds when 0&le;<code>i</code>&lt;<code>size()</code>. Otherwise, the result is either 
   * undefined or an exception is thrown. */
  public JsonString getName(int i)
  {
    return names[i];
  }
  
  /** Returns the value of the field located at the specified index without boundary checking. 
   * This method suceeds when 0&le;<code>i</code>&lt;<code>size()</code>. Otherwise, the result 
   * is either undefined or an exception is thrown. */
  public JsonValue get(int i)
  {
    return values[i];
  }

  /** Returns the internal array used to store field names. Do not modify! Field names might 
   * be sorted or indexed. */ 
  public JsonString[] getInternalNamesArray() {
    return names;
  }
  
  /** Returns the internal array used to store values */
  public JsonValue[] getInternalValuesArray() {
    return values;
  }


  // -- mutation ----------------------------------------------------------------------------------


  /** Adds the specified (name, value) pair to this record (without copying). Throws an exception if 
   * a field of the specified name exists already.*/
  public void add(JsonString name, JsonValue value)
  {
  	addOrSet(name, value, true);
  }

  /** Adds or updates the specified (name, value) pair (without copying). */
  public void set(JsonString name, JsonValue value)
  {
    addOrSet(name, value, false);
  }
  
  /** Sets the value of the field with the specified index. without boundary checking. 
   * This method suceeds when 0&le;<code>i</code>&lt;<code>size()</code>. Otherwise, the result 
   * is either undefined or an exception is thrown. */
  public void set(int i, JsonValue value)
  {
    values[i] = value; 
  }
  
  /** Sets the internal arrays to new values. No consistency checks are performed. The arguments 
   * are not copied. Use with caution! */
  public void setInternal(JsonString[] names, JsonValue[] values, int arity, boolean isSorted) {
    assert names.length >= arity;
    assert values.length >= arity;
    this.names = names;
    this.values = values;
    this.size = arity;
    this.isSorted = isSorted;
    reindex();
  }
  
  /** Adds or updates the specified (name, value) pair (without copying).
   * 
   * @param exceptionOnSet throw RuntimeException when field already present?
   */
  protected void addOrSet(JsonString name, JsonValue value, boolean exceptionOnSet) {
  	int index = indexOf(name);
  	if (index >= 0) {
  		if (exceptionOnSet) {
  			throw new RuntimeException("duplicate field name: " + name);
  		} else {
  			set(index, value);
  		}
  	}	else {
  		index = size;
  	  resize(size+1);
  		names[index] = name;
      values[index] = value;
      hashIndex.put(name, index);
      isSorted = index==0 || (isSorted && names[index].compareTo(names[index-1]) > 0);
    }
  }

  /** Clears this record, i.e., removes all its fields. */
  public void clear()
  {
    this.size = 0;
    hashIndex.clear();
    isSorted = true;
  }

  /** Increases the capacity of this record so that it can hold more fields, but does not change
   * its actual size. */
  public void ensureCapacity(int capacity)
  {
    if (names.length  < capacity)
    {
      int newCapacity = Math.max(MIN_CAPACITY, names.length);
      while (newCapacity < capacity) newCapacity *= 2;
      
      JsonString[] newNames = new JsonString[newCapacity];
      System.arraycopy(names, 0, newNames, 0, names.length);
      names = newNames;
      JsonValue[] newValues = new JsonValue[newCapacity];
      System.arraycopy(values, 0, newValues, 0, values.length);
      values = newValues;
    }
  }

  /** Resizes this record to the specified size. If <code>newSize<size()</code> the field names
   * with the largest index are truncated. Otherwise, undefined names/values are appended; the 
   * calling method has to make sure that the record stays valid. */
  protected void resize(int newSize)
  {
    ensureCapacity(newSize);
    this.size = newSize;
  }
  
  /** Copies the content of the given record into this record. */
  public void setCopy(JsonRecord other) throws Exception
  {
    clear();
    resize(other.size());
    if (other instanceof BufferedJsonRecord)
    {
      BufferedJsonRecord o = (BufferedJsonRecord)other;
      for (int i=0; i<size; i++)
      {
        names[i] = o.names[i].getCopy(names[i]); // TODO: copy names?
        values[i] = JsonUtil.getCopy(o.values[i], values[i]);
      }
      isSorted = o.isSorted;
      hashIndex.clear();
      hashIndex.putAll(((BufferedJsonRecord)other).hashIndex);
    }
    else 
    {
      // other record impementation
      int i=0;
      for (Entry<JsonString, JsonValue> e : other)
      {
        names[i] = e.getKey().getCopy(names[i]); // TODO: copy names?
        values[i] = JsonUtil.getCopy(e.getValue(), values[i]);
        isSorted = i==0 || (isSorted && names[i].compareTo(names[i-1])>0);
        i++;
      }
      reindex();
    }
  }
  
  /** Rearranges the names to be in sorted order. This will change the indexes of some fields! */
  public void sort()
  {
    Integer[] sortedIndex = sortPermutation();
    JsonString name = null;
    JsonValue value = null;
    for (int i=0; i<size; i++)
    {
      if (sortedIndex[i] == null) continue; // already moved
      name = names[i]; // cache
      value = values[i]; // cache
      int j = i;
      while (sortedIndex[j] != i) // until one permutation cycle is completed
      {
        names[j] = names[sortedIndex[j]];
        values[j] = values[sortedIndex[j]];
        int k = sortedIndex[j];
        sortedIndex[j] = null; // mark as done
        j = k;
      }
      names[j] = name;
      values[j] = value;
      sortedIndex[j] = null; // mark as done
    }
    isSorted = true;
    reindex();
  }
  
  /** Computes the permutation of the fields that leads to field-name order. */
  protected Integer[] sortPermutation()
  {
    Integer[] p = new Integer[size];
    for (int i=0; i<size; i++)
    {
      p[i] = i;
    }
    Arrays.sort(p, new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2)
      {
        return names[o1].compareTo(names[o2]);
      }
    });
    return p;
  }
  
  /** Recomputes the hash index */
  protected void reindex()
  {
    hashIndex.clear();
    for (int i=0; i<size; i++)
    {
      hashIndex.put(names[i], i);
    }
  }
  
  // -- Iterable interface ------------------------------------------------------------------------
  
  /** Returns an iterator over the fields in this record (in index order). */
  @Override
  public Iterator<Entry<JsonString, JsonValue>> iterator()
  {
    return new Iterator<Entry<JsonString, JsonValue>>()
    {
      int i = 0;
      RecordEntry entry = new RecordEntry(); // reused
      
      @Override
      public boolean hasNext()
      {
        return i < size();
      }

      @Override
      public Entry<JsonString, JsonValue> next()
      {
        entry.name = names[i]; 
        entry.value = values[i];
        i++;
        return entry;
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();        
      }      
    };
  }
  
  /** Returns an iterator over the fields in this record (in field-name order). */
  public Iterator<Entry<JsonString, JsonValue>> iteratorSorted()
  {
    if (isSorted) return iterator();
    
    final Integer[] sortedIndex = sortPermutation();
    
    // and return an iterator using that index
    return new Iterator<Entry<JsonString, JsonValue>>()
    {
      int i = 0;
      RecordEntry entry = new RecordEntry(); // reused
      
      @Override
      public boolean hasNext()
      {
        return i < size();
      }

      @Override
      public Entry<JsonString, JsonValue> next()
      {
        entry.name = names[sortedIndex[i]];
        entry.value = values[sortedIndex[i]];
        i++;
        return entry;
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();        
      }      
    };
  }


  
  // -- misc --------------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.MEMORY_RECORD;
  }

}
