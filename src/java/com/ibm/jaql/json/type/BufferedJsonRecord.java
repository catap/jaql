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

import com.ibm.jaql.util.BaseUtil;


/** An in-memory {@link JsonRecord}. */
public class BufferedJsonRecord extends JsonRecord {

	protected final static JsonString[] NO_NAMES  = new JsonString[0];
  protected final static JsonValue[]  NO_VALUES = new JsonValue[0];        
  
  // names and values are parallel arrays with names being kept in sorted order
  protected JsonString[] 					 names      = NO_NAMES;			 
  protected JsonValue[] 						 values     = NO_VALUES;		   
  protected int                  arity      = 0;

  
  /**
   * 
   */
  public BufferedJsonRecord()
  {
  }

  /**
   * @param capacity
   */
  public BufferedJsonRecord(int capacity)
  {
    ensureCapacity(capacity);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.MEMORY_RECORD;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#arity()
   */
  @Override
  public int arity()
  {
    return this.arity;
  }

  /**
   * @param arity
   */
  protected void setArity(int arity)
  {
    ensureCapacity(arity);
    this.arity = arity;
  }

	/** @see Arrays#binarySearch(Object[], int, int, Object) */
  protected int binarySearch(JsonString name) {
  	return arity > 0 ? Arrays.binarySearch(names, 0, arity, name) : -1;
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#findName(com.ibm.jaql.json.type.JString)
   */
  @Override
  public int findName(JsonString name)
  {
  	return binarySearch(name);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#findName(java.lang.String)
   */
  @Override
  public int findName(String name)
  {
    // TODO: conversion from String to JString ok?
    return findName(new JsonString(name));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getName(int)
   */
  @Override
  public JsonString getName(int i)
  {
    return names[i];
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getValue(int)
   */
  @Override
  public JsonValue getValue(int i)
  {
    return values[i];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getValue(com.ibm.jaql.json.type.JString,
   *      com.ibm.jaql.json.type.Item)
   */
  @Override
  public JsonValue getValue(JsonString name, JsonValue defaultValue)
  {
  	int index = findName(name);
  	return index >= 0 ? values[index] : defaultValue;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getValue(java.lang.String,
   *      com.ibm.jaql.json.type.Item)
   */
  @Override
  public JsonValue getValue(String name, JsonValue defaultValue)
  {
  	int index = findName(name);
  	return index >= 0 ? values[index] : defaultValue;
  }

  /**
   * 
   */
  public void clear()
  {
    this.arity = 0;
  }

  /**
   * name and value now belong to this record.
   * 
   * @param name
   * @param value
   */
  public void add(JsonString name, JsonValue value)
  {
  	addOrSet(name, value, true);
  }

  /**
   * name and value now belong to this record.
   * 
   * @param name
   * @param value
   */
  public void addOrSet(JsonString name, JsonValue value) {
  	addOrSet(name, value, false);
  }

  /**
   * name and value now belong to this record.
   * 
   * @param name
   * @param cachedValue
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
  		setArity(arity+1);
  		index = -index-1;
  		if (index < arity-1) {
  			System.arraycopy(names, index, names, index+1, (arity-1)-index);
  			System.arraycopy(values, index, values, index+1, (arity-1)-index);
  		}
  		names[index] = name;
  		set(index, value);
  		reorg();
  	}
  }
  
  /**
   * @param name
   * @param value
   */
  public void add(String name, JsonValue value)
  {
    add(new JsonString(name), value); // TODO: memory
  }

  /**
   * @param i
   * @param value
   */
  public void set(int i, JsonValue value)
  {
    values[i] = value; // TODO: unsafe to call readfields now because we don't own this value
  }

    /** Adds or sets
   * @param name
   * @param value
   */
  public void set(JsonString name, JsonValue value)
  {
  	addOrSet(name, value);
  }

  /**
   * @param name
   * @param value
   */
  public void set(String name, JsonValue value)
  {
    set(new JsonString(name), value); // TODO: memory
  }

  /**
   * @return
   */
  public int getCapacity()
  {
    return names.length;
  }

  /**
   * @param minCapacity
   */
  public void ensureCapacity(int minCapacity)
  {
  	int curCapacity = getCapacity();
  	if (curCapacity  < minCapacity)
    {
      JsonString[] newNames = new JsonString[minCapacity];
      System.arraycopy(names, 0, newNames, 0, curCapacity);
      names = newNames;
      JsonValue[] newValues = new JsonValue[minCapacity];
      System.arraycopy(values, 0, newValues, 0, curCapacity);
      values = newValues;
      for (int i = curCapacity; i < minCapacity; i++) {
        names[i] = new JsonString();
        //values[i] = null;
      }
      // no reorg needed because all fields have the same positions
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#hashCode()
   */
  public int hashCode()
  {
  	long h = BaseUtil.GOLDEN_RATIO_64;
    for (int i = 0; i < arity; i++)
    {
      h |= names[i].hashCode();
      h *= BaseUtil.GOLDEN_RATIO_64;
      h |= values[i].hashCode();
      h *= BaseUtil.GOLDEN_RATIO_64;
    }
    return (int) (h >> 32);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#equals(java.lang.Object)
   */
  public boolean equals(Object x)
  {
    return this.compareTo(x) == 0; // goes to JRecord
  }

  @Override
	public void setCopy(JsonValue value) throws Exception {
		JsonRecord r = (JsonRecord) value;
    int n = r.arity();
    ensureCapacity(n);
    arity = n;
    for (int i = 0; i < n; i++)
    {
      names[i].setCopy(r.getName(i));
      JsonValue v = r.getValue(i); 
      values[i] = v==null ? null : v.getCopy(values[i]);
    }
    reorg(); 		
	} 

  /** Called whenever names have changed positions. Used by subclasses. */
  protected void reorg() {
  	// all methods maintain sorted order --> no actions required
    // TODO: it might be more efficient to start unsorted and to sort only on demand
    //       (an isSorted flag might be kept)
  }
  
  /** Returns the internal array used to store field names */ 
  public JsonString[] getInternalNamesArray() {
    return names;
  }
  
  /** Returns the internal array used to store values */
  public JsonValue[] getInternalValuesArray() {
    return values;
  }
  
  /** Sets the internal arrays to new values. The names have to be sorted; no consistency
   * checks are performed. The arguments are not copied. */
  public void set(JsonString[] names, JsonValue[] values, int arity) {
    assert names.length >= arity;
    assert values.length >= arity;
    this.names = names;
    this.values = values;
    this.arity = arity;
  }
}
