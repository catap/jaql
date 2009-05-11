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

import com.ibm.jaql.json.meta.MetaArray;
import com.ibm.jaql.json.util.Iter;

/**
 * 
 */
public class JavaJArray extends JArray
{
  protected MetaArray meta;
  protected Object    value;
  protected Item      buffer;

  /**
   * 
   */
  public JavaJArray()
  {
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.JAVA_ARRAY;
  }

  /**
   * @param value
   */
  public void setObject(Object value)
  {
    if (meta == null || meta.getClazz() != value.getClass())
    {
      meta = MetaArray.getMetaArray(value.getClass());
      buffer = meta.makeItem();
    }
    this.value = value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#count()
   */
  @Override
  public long count()
  {
    return meta.count(value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#getTuple(com.ibm.jaql.json.type.Item[])
   */
  @Override
  public void getTuple(Item[] items) throws Exception
  {
    Item item;
    Iter iter = meta.iter(value);
    for (int i = 0; i < items.length; i++)
    {
      item = iter.next();
      if (item == null)
      {
        throw new RuntimeException("expected exactly " + items.length
            + " but found less");
      }
      if (items[i] == null)
      {
        items[i] = new Item();
      }
      items[i].setCopy(item);
    }
    if (iter.next() != null)
    {
      throw new RuntimeException("expected exactly " + items.length
          + " but found more");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#iter()
   */
  @Override
  public Iter iter() throws Exception
  {
    return meta.iter(value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#nth(long)
   */
  @Override
  public Item nth(long n) throws Exception
  {
    meta.nth(value, n, buffer);
    return buffer;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataInput)
   */
  @Deprecated
  public void readFields(DataInput in) throws IOException
  {
    String className = in.readUTF(); // TODO: would like to compress out the class name...
    if (meta == null || !meta.getClazz().getName().equals(className))
    {
      meta = MetaArray.getMetaArray(className);
      value = meta.newInstance();
      buffer = meta.makeItem();
    }
    this.value = meta.read(in, this.value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#write(java.io.DataOutput)
   */
  @Deprecated
  public void write(DataOutput out) throws IOException
  {
    out.writeUTF(meta.getClazz().getName()); // TODO: would like to compress out the class name...
    meta.write(out, value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JValue jvalue) throws Exception
  {
    JavaJRecord that = (JavaJRecord) jvalue;
    this.value = meta.copy(this.value, that.value);
  }

}
