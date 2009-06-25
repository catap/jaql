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

import java.util.HashMap;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;

/** Common API for accessing Java arrays. Each subclass implements these methods for a
 * specific (atomic) type. */
public abstract class MetaArray extends MetaValue
{
  protected Class<?> clazz;

  /**
   * You must call initialize() after constructing (and AFTER placing into the
   * MetaArray factory).
   * 
   * @param clazz
   */
  protected MetaArray(Class<?> clazz)
  {
    this.clazz = clazz;
    metaMap.put(clazz.getName(), this);
  }

  /**
   * @return
   */
  public Class<?> getClazz()
  {
    return clazz;
  }

  /**
   * @return
   */
  public abstract Item makeItem();
  /**
   * @param obj a Java array
   * @return
   */
  public abstract long count(Object obj);
  /**
   * @param obj a Java array
   * @return
   * @throws Exception
   */
  public abstract Iter iter(Object obj) throws Exception;
  /**
   * @param obj a Java array
   * @param n
   * @param result
   * @throws Exception
   */
  public abstract void nth(Object obj, long n, Item result) throws Exception;
  /**
   * @param toValue a Java array
   * @param fromValue a Java array
   * @return
   */
  public abstract Object copy(Object toValue, Object fromValue);

  /**
   * 
   */
  private static HashMap<String, MetaArray> metaMap = new HashMap<String, MetaArray>();

  /**
   * @param className
   * @param meta
   */
  public static void setMetaArray(String className, MetaArray meta)
  {
    metaMap.put(className, meta);
  }

  /**
   * @param clazz
   * @param meta
   */
  public static void setMetaArray(Class<?> clazz, MetaArray meta)
  {
    metaMap.put(clazz.getName(), meta);
  }

  static
  {
    new BooleanMetaArray();
    new ByteMetaArray();
    new CharMetaArray();
    new DoubleMetaArray();
    new FloatMetaArray();
    new IntMetaArray();
    new LongMetaArray();
    new ShortMetaArray();
    new StringMetaArray();
  }

  /**
   * @param clazz
   * @return
   */
  public static MetaArray getMetaArray(Class<?> clazz)
  {
    return getMetaArray(clazz.getName());
  }

  /**
   * @param className
   * @return
   */
  public static MetaArray getMetaArray(String className)
  {
    MetaArray meta = metaMap.get(className);
    if (meta == null)
    {
      throw new RuntimeException("unknown MetaArray type: " + className);
    }
    return meta;
  }
}
