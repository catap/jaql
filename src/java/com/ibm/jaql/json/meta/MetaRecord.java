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

import java.lang.reflect.UndeclaredThrowableException;
import java.util.HashMap;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;

/**
 * 
 */
public abstract class MetaRecord extends MetaValue
{
  protected Class<?> clazz;

  /**
   * @param clazz
   */
  protected MetaRecord(Class<?> clazz)
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
  public abstract int arity();
  /**
   * @return
   */
  public abstract Item[] makeItems();
  /**
   * @param name
   * @return
   */
  public abstract int findField(JString name);
  /**
   * @param name
   * @return
   */
  public abstract int findField(String name);
  /**
   * @param fieldId
   * @return
   */
  public abstract JString getName(int fieldId);
  /**
   * @param obj
   * @param fieldId
   * @param result
   */
  public abstract void getValue(Object obj, int fieldId, Item result);
  /**
   * @param toValue
   * @param fromValue
   */
  public abstract void copy(Object toValue, Object fromValue);

  /**
   * 
   */
  private static HashMap<String, MetaRecord> metaMap = new HashMap<String, MetaRecord>();

  /**
   * @param className
   * @param meta
   */
  public static void setMetaRecord(String className, MetaRecord meta)
  {
    metaMap.put(className, meta);
  }

  /**
   * @param clazz
   * @return
   */
  public static MetaRecord getMetaRecord(Class<?> clazz)
  {
    String className = clazz.getName();
    MetaRecord meta = metaMap.get(className);
    if (meta == null)
    {
      meta = new ReflectionMetaRecord(clazz);
    }
    return meta;
  }

  /**
   * @param className
   * @return
   */
  public static MetaRecord getMetaRecord(String className)
  {
    MetaRecord meta = metaMap.get(className);
    if (meta == null)
    {
      Class<?> clazz;
      try
      {
        clazz = Class.forName(className);
      }
      catch (ClassNotFoundException e)
      {
        throw new UndeclaredThrowableException(e);
      }
      meta = new ReflectionMetaRecord(clazz);
      metaMap.put(className, meta);
    }
    return meta;
  }
}
