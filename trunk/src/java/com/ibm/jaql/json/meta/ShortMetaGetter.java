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
import java.lang.reflect.Method;

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;

/**
 * 
 */
public final class ShortMetaGetter extends MetaGetter
{
  /**
   * @param name
   * @param getter
   * @param setter
   */
  public ShortMetaGetter(String name, Method getter, Method setter)
  {
    super(name, getter, setter);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#makeItem()
   */
  @Override
  public JsonLong makeValue()
  {
    return new JsonLong();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#get(java.lang.Object,
   *      com.ibm.jaql.json.type.Item)
   */
  @Override
  public JsonValue get(Object obj, JsonValue target) throws IllegalArgumentException,
      IllegalAccessException, InvocationTargetException
  {
    short x = (Short) getter.invoke(obj);
    ((JsonLong) target).value = x;
    return target;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#read(java.io.DataInput,
   *      java.lang.Object)
   */
  @Override
  public void read(DataInput in, Object obj) throws IOException,
      IllegalArgumentException, IllegalAccessException,
      InvocationTargetException
  {
    short x = in.readShort();
    setter.invoke(obj, x);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#write(java.io.DataOutput,
   *      java.lang.Object)
   */
  @Override
  public void write(DataOutput out, Object obj) throws IOException,
      IllegalArgumentException, IllegalAccessException,
      InvocationTargetException
  {
    short x = (Short) getter.invoke(obj);
    out.writeShort(x);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#copy(java.lang.Object,
   *      java.lang.Object)
   */
  @Override
  public void copy(Object toObject, Object fromObject)
      throws IllegalArgumentException, IllegalAccessException,
      InvocationTargetException
  {
    short x = (Short) getter.invoke(fromObject);
    setter.invoke(toObject, x);
  }
}
