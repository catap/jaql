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
import java.lang.reflect.Field;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;

/**
 * 
 */
public class StringMetaField extends MetaField
{
  /**
   * @param name
   * @param field
   */
  public StringMetaField(String name, Field field)
  {
    super(name, field);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#makeItem()
   */
  @Override
  public Item makeItem()
  {
    return new Item(new JString());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaAccessor#get(java.lang.Object,
   *      com.ibm.jaql.json.type.Item)
   */
  @Override
  public void get(Object obj, Item target) throws IllegalArgumentException,
      IllegalAccessException
  {
    String s = (String) field.get(obj);
    ((JString) target.get()).set(s);
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
    String s = in.readUTF();
    field.set(obj, s);
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
    String s = (String) field.get(obj);
    out.writeUTF(s);
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
    Object s = field.get(fromObject);
    field.set(toObject, s);
  }
}
