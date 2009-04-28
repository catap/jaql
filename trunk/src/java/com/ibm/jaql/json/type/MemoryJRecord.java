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
import java.util.Arrays;

import com.ibm.jaql.util.BaseUtil;


/** An in-memory {@link JRecord}. */
public class MemoryJRecord extends JRecord {

	protected final static JString[] NO_NAMES = new JString[0];
  protected final static Item[] 	 NO_ITEMS = new Item[0];        
  
  // names and values are parallel arrays with names being kept in sorted order
  protected JString[] 					 names      = NO_NAMES;			 
  protected Item[]  						 values     = NO_ITEMS;		   
  protected int                  arity      = 0;

  
  /**
   * 
   */
  public MemoryJRecord()
  {
  }

  /**
   * @param capacity
   */
  public MemoryJRecord(int capacity)
  {
    ensureCapacity(capacity);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.MEMORY_RECORD;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#arity()
   */
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
  protected int binarySearch(JString name) {
  	return arity > 0 ? Arrays.binarySearch(names, 0, arity, name) : -1;
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#findName(com.ibm.jaql.json.type.JString)
   */
  public int findName(JString name)
  {
  	return binarySearch(name);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#findName(java.lang.String)
   */
  public int findName(String name)
  {
    // TODO: conversion from String to JString ok?
    return findName(new JString(name));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getName(int)
   */
  public JString getName(int i)
  {
    return names[i];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getValue(int)
   */
  public Item getValue(int i)
  {
    return values[i];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getValue(com.ibm.jaql.json.type.JString,
   *      com.ibm.jaql.json.type.Item)
   */
  public Item getValue(JString name, Item dfault)
  {
  	int index = findName(name);
  	return index >= 0 ? values[index] : dfault;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#getValue(java.lang.String,
   *      com.ibm.jaql.json.type.Item)
   */
  public Item getValue(String name, Item dfault)
  {
  	int index = findName(name);
  	return index >= 0 ? values[index] : dfault;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException
  {
    int arity = BaseUtil.readVUInt(in);
    setArity(arity);
    for (int i = 0; i < arity; i++)
    {
      names[i].readFields(in);
      values[i].readFields(in);
    }
//    reorg();		// not needed because input records are already sorted
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JRecord#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException
  {
    BaseUtil.writeVUInt(out, arity);
    for (int i = 0; i < arity; i++)
    {
      names[i].write(out);
      values[i].write(out);
    }
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
  public void add(JString name, Item value)
  {
  	addOrSet(name, value, true);
  }

  /**
   * name and value now belong to this record.
   * 
   * @param name
   * @param value
   */
  public void addOrSet(JString name, Item value) {
  	addOrSet(name, value, false);
  }

  /**
   * name and value now belong to this record.
   * 
   * @param name
   * @param value
   * @param exceptionOnSet throw RuntimeException when field already present?
   */
  protected void addOrSet(JString name, Item item, boolean exceptionOnSet) {
  	int index = binarySearch(name);
  	if (index >= 0) {
  		if (exceptionOnSet) {
  			throw new RuntimeException("duplicate field name: " + name);
  		} else {
  			set(index, item);
  		}
  	}	else {
  		setArity(arity+1);
  		index = -index-1;
  		if (index < arity-1) {
  			System.arraycopy(names, index, names, index+1, (arity-1)-index);
  			System.arraycopy(values, index, values, index+1, (arity-1)-index);
  		}
  		names[index] = name;
  		set(index, item);
  		reorg();
  	}
  }
  
  /**
   * @param name
   * @param value
   */
  public void add(String name, Item value)
  {
    add(new JString(name), value); // TODO: memory
  }

  /**
   * @param name
   * @param value
   */
  public void add(JString name, JValue value)
  {
    add(name, new Item(value)); // TODO: memory
  }

  /**
   * @param name
   * @param value
   */
  public void add(String name, JValue value)
  {
    add(name, new Item(value)); // TODO: memory
  }

  /**
   * @param i
   * @param value
   */
  public void set(int i, Item value)
  {
    values[i] = value; // TODO: unsafe to call readfields now because we don't own this value
  }

  /**
   * @param i
   * @param value
   */
  public void set(int i, JValue value) {
    set(i, new Item(value)); // TODO: memory
  }

  /** Adds or sets
   * @param name
   * @param value
   */
  public void set(JString name, Item value)
  {
  	addOrSet(name, value);
  }

  /**
   * @param name
   * @param value
   */
  public void set(String name, Item value)
  {
  	set(new JString(name), value); // TODO: memory
  }

  /**
   * @param name
   * @param value
   */
  public void set(JString name, JValue value)
  {
    set(name, new Item(value)); // TODO: memory
  }

  /**
   * @param name
   * @param value
   */
  public void set(String name, JValue value)
  {
    set(name, new Item(value)); // TODO: memory
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
      JString[] newNames = new JString[minCapacity];
      System.arraycopy(names, 0, newNames, 0, curCapacity);
      names = newNames;
      Item[] newItems = new Item[minCapacity];
      System.arraycopy(values, 0, newItems, 0, curCapacity);
      values = newItems;
      for (int i = curCapacity; i < minCapacity; i++) {
        names[i] = new JString();
        values[i] = new Item();
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
	public void setCopy(JValue value) throws Exception {
		JRecord r = (JRecord) value;
    int n = r.arity();
    ensureCapacity(n);
    arity = n;
    for (int i = 0; i < n; i++)
    {
      names[i].setCopy(r.getName(i));
      values[i].setCopy(r.getValue(i));
    }
    reorg(); 		
	} 

  /** Called whenever names have changed positions. Used by subclasses. */
  protected void reorg() {
  	// all methods maintain sorted order --> no actions required
  }
}
