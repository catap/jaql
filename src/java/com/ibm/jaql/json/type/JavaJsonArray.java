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

import com.ibm.jaql.json.meta.MetaArray;
import com.ibm.jaql.json.util.JsonIterator;

/**
 * 
 */
//TODO: cleanup
public class JavaJsonArray extends JsonArray 
{
  protected MetaArray metaArray;
  protected Object    value;
  protected JsonValue buffer;

  /**
   * 
   */
  public JavaJsonArray()
  {
  }

  public void setClass(String className)
  {
    if (metaArray == null || !metaArray.getClazz().getName().equals(className))
    {
      metaArray = MetaArray.getMetaArray(className);
      value = metaArray.newInstance();
      buffer = metaArray.makeValue();
    }
  }

  public MetaArray getMetaArray()
  {
    return metaArray; 
  }

  public Object getValue()
  {
    return value; 
  }
  
  public void setValue(Object value)
  {
    assert value.getClass().equals(metaArray.getClazz());
    this.value = value;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.JAVA_ARRAY;
  }

  /**
   * @param value
   */
  public void setObject(Object value)
  {
    if (metaArray == null || metaArray.getClazz() != value.getClass())
    {
      metaArray = MetaArray.getMetaArray(value.getClass());
      buffer = metaArray.makeValue();
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
    return metaArray.count(value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#getTuple(com.ibm.jaql.json.type.Item[])
   */
  @Override
  public void getAll(JsonValue[] values) throws Exception
  {
    JsonIterator iter = metaArray.iter(value);
    for (int i = 0; i < values.length; i++)
    {
      if (!iter.moveNext())
      {
        throw new RuntimeException("expected exactly " + values.length
            + " but found less");
      }
      JsonValue value = iter.current();
      values[i] = JsonUtil.getCopy(value, values[i]);
    }
    if (iter.moveNext())
    {
      throw new RuntimeException("expected exactly " + values.length
          + " but found more");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#iter()
   */
  @Override
  public JsonIterator iter() throws Exception
  {
    return metaArray.iter(value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JArray#nth(long)
   */
  @Override
  public JsonValue nth(long n) throws Exception
  {
    buffer = metaArray.nth(value, n, buffer);
    return buffer;
  }


  @Override
  public JavaJsonArray getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    JavaJsonArray t;
    if (target instanceof JavaJsonArray)
    {
      t = (JavaJsonArray) target;
    }
    else
    {
      t = new JavaJsonArray();
    }
    
    t.metaArray = this.metaArray;
    t.buffer = metaArray.makeValue();
    t.value = metaArray.copy(t.value, this.value);
    return t;  
  }
}
