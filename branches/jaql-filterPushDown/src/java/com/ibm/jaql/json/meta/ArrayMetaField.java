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
 */package com.ibm.jaql.json.meta;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;

import com.ibm.jaql.json.type.JavaJsonArray;
import com.ibm.jaql.json.type.JsonValue;

/**
 * 
 */
public class ArrayMetaField extends MetaField
{
  protected MetaArray meta;

  /**
   * @param name
   * @param field
   */
  public ArrayMetaField(String name, Field field)
  {
    super(name, field);
    meta = MetaArray.getMetaArray(field.getType());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#makeItem()
   */
  @Override
  public JavaJsonArray makeValue()
  {
    return new JavaJsonArray();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#get(java.lang.Object,
   *      com.ibm.jaql.json.type.Item)
   */
  @Override
  public JsonValue get(Object obj, JsonValue target) throws IllegalArgumentException,
      IllegalAccessException
  {
    Object x = field.get(obj);
    if (x == null)
    {
      return null;
    }
    else
    {
      ((JavaJsonArray) target).setObject(x);
      return target;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#read(java.io.DataInput,
   *      java.lang.Object)
   */
  @Override
  public void read(DataInput in, Object obj) throws IOException,
      IllegalArgumentException, IllegalAccessException
  {
    if (in.readByte() == 0)
    {
      field.set(obj, null);
    }
    else
    {
      Object value = field.get(obj);
      Object newValue = meta.read(in, value);
      if (newValue != value)
      {
        field.set(obj, newValue);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#write(java.io.DataOutput,
   *      java.lang.Object)
   */
  @Override
  public void write(DataOutput out, Object obj) throws IOException,
      IllegalArgumentException, IllegalAccessException
  {
    Object value = field.get(obj);
    if (value == null)
    {
      out.writeByte(0);
    }
    else
    {
      out.writeByte(1);
      meta.write(out, value);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#copy(java.lang.Object,
   *      java.lang.Object)
   */
  @Override
  public void copy(Object toObject, Object fromObject)
      throws IllegalArgumentException, IllegalAccessException
  {
    Object fromValue = field.get(fromObject);
    Object toValue = field.get(toObject);
    meta.copy(toValue, fromValue);
  }
}
