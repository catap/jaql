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
import java.util.Iterator;
import java.util.Map.Entry;


/** An in-memory {@link JsonRecord}. 
 * 
 * In addition to name-based record access, this class provides functionality to access record
 * fields by index. This is possible because this record is implemented using two parallel arrays:
 * one storing the field names and one storing the actual values. (The field-name array is kept 
 * sorted for efficiency reasons.) Thus, as long as the record's fields are not modified, field 
 * indexes are stable and can be used to avoid the look-up cost of name-based access.
 */
public class BufferedJsonRecord extends JsonRecord {
	protected final static JsonString[] NO_NAMES  = new JsonString[0];
  protected final static JsonValue[]  NO_VALUES = new JsonValue[0];        
  
  // names and values are parallel arrays with names being kept in sorted order
  protected JsonString[]     names   = NO_NAMES;			 
  protected JsonValue[]      values  = NO_VALUES;		   
  protected int              size    = 0;


  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs an empty in-memory JSON record */
  public BufferedJsonRecord()
  {
  }

  /** Constructs an empty in-memory JSON record and allocates space for the specified number of
   * entries */
  public BufferedJsonRecord(int capacity)
  {
    ensureCapacity(capacity);
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
    
    // and copy
    for (int i = 0; i < size; i++)
    {
      t.names[i] = JsonUtil.getCopy(names[i], t.names[i]); 
      t.values[i] = JsonUtil.getCopy(values[i], t.values[i]);
    }
    t.reorg();    
    return t;
  } 


  // -- index-based access ------------------------------------------------------------------------

  /** @see Arrays#binarySearch(Object[], int, int, Object) */
  protected int binarySearch(JsonString name) {
    return size > 0 ? Arrays.binarySearch(names, 0, size, name) : -1;
  }

  /** Searches this record for the specified field and returns its index. Returns a negative
   * number when the field name has not been found. */
  public int indexOf(JsonString name)
  {
  	return binarySearch(name);
  }

  /** Returns the field located at the specified index without boundary checking. This
   * method suceeds when 0&le;<code>i</code>&lt;<code>size()</code>. Otherwise, the result is either 
   * undefined or an exception is thrown. */
  public JsonString nameOf(int i)
  {
    return names[i];
  }
  
  /** Returns the value of the field located at the specified index without boundary checking. 
   * This method suceeds when 0&le;<code>i</code>&lt;<code>size()</code>. Otherwise, the result 
   * is either undefined or an exception is thrown. */
  public JsonValue valueOf(int i)
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


  /** Adds the specified (name, value) pair to this record (without copying). Throws an expection if 
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
    values[i] = value; // TODO: unsafe to call readfields now because we don't own this value
  }
  
  /** Sets the internal arrays to new values. The names have to be sorted; no consistency
   * checks are performed. The arguments are not copied. Use with caution! */
  public void setInternal(JsonString[] names, JsonValue[] values, int arity) {
    assert names.length >= arity;
    assert values.length >= arity;
    this.names = names;
    this.values = values;
    this.size = arity;
  }
  
  /** Adds or updates the specified (name, value) pair (without copying).
   * 
   * @param exceptionOnSet throw RuntimeException when field already present?
   */
  protected void addOrSet(JsonString name, JsonValue value, boolean exceptionOnSet) {
  	int index = binarySearch(name);
  	if (index >= 0) {
  		if (exceptionOnSet) {
  			throw new RuntimeException("duplicate field name: " + name);
  		} else {
  			set(index, value);
  		}
  	}	else {
  		resize(size+1);
  		index = -index-1;
  		if (index < size-1) {
  			System.arraycopy(names, index, names, index+1, (size-1)-index);
  			System.arraycopy(values, index, values, index+1, (size-1)-index);
  		}
  		names[index] = name;
  		set(index, value);
  		reorg();
  	}
  }

  /** Clears this record, i.e., removes all its fields. */
  public void clear()
  {
    this.size = 0;
  }

  /** Increases the capacity of this record so that it can hold more fields, but does not change
   * its actual size. */
  public void ensureCapacity(int capacity)
  {
    int curCapacity = names.length;
    if (curCapacity  < capacity)
    {
      JsonString[] newNames = new JsonString[capacity];
      System.arraycopy(names, 0, newNames, 0, curCapacity);
      names = newNames;
      JsonValue[] newValues = new JsonValue[capacity];
      System.arraycopy(values, 0, newValues, 0, curCapacity);
      values = newValues;
      for (int i = curCapacity; i < capacity; i++) {
        names[i] = new JsonString();
        //values[i] = null;
      }
      // no reorg needed because all fields have the same positions
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

  /** Called whenever names have changed positions. Used by subclasses. */
  protected void reorg() {
  	// all methods maintain sorted order --> no actions required
    // TODO: it might be more efficient to start unsorted and to sort only on demand
    //       (an isSorted flag might be kept)
  }
  
  /** Copies the content of the given record into this record. */
  public void setCopy(JsonRecord other) throws Exception
  {
    clear();
    resize(other.size());
    int i=0;
    for (Entry<JsonString, JsonValue> e : other)
    {
      names[i] = e.getKey().getCopy(names[i]);
      values[i] = e.getValue().getCopy(values[i]);
      i++;
    }
    reorg();    
  }
  
  
  // -- Iterable interface ------------------------------------------------------------------------
  
  /** Returns an iterator over the fields in this record */
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
        entry.name = nameOf(i);
        entry.value = valueOf(i);
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
