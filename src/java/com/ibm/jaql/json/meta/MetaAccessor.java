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
package com.ibm.jaql.json.meta;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;

/** Common API for accessing atomic values stored within a Java object. The API includes
 * methods to read the value, write the value, copy the value, and convert the value to 
 * an item. Subclasses implement the API for specific types of Java objects.  
 */
public abstract class MetaAccessor
{
  private JString name;

  /**
   * @param name
   */
  protected MetaAccessor(JString name)
  {
    this.name = name;
  }

  /**
   * @param name
   */
  protected MetaAccessor(String name)
  {
    this.name = new JString(name);
  }

  /**
   * @return
   */
  public final JString getName()
  {
    return name;
  }

  /**
   * @return
   */
  public abstract Item makeItem();
  /**
   * @param obj
   * @param target
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  public abstract void get(Object obj, Item target)
      throws IllegalArgumentException, IllegalAccessException,
      InvocationTargetException;
  /**
   * @param in
   * @param obj
   * @throws IOException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  public abstract void read(DataInput in, Object obj) throws IOException,
      IllegalArgumentException, IllegalAccessException,
      InvocationTargetException;
  /**
   * @param out
   * @param obj
   * @throws IOException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  public abstract void write(DataOutput out, Object obj) throws IOException,
      IllegalArgumentException, IllegalAccessException,
      InvocationTargetException;
  /**
   * @param toObject
   * @param fromObject
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  public abstract void copy(Object toObject, Object fromObject)
      throws IllegalArgumentException, IllegalAccessException,
      InvocationTargetException;
}
