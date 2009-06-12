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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.TreeMap;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

/** MetaRecord in which the (name, value)-pairs correspond to the public variables and 
 * public getter/setter methods (of form T getX() and setX(T), where X is the name and T
 * the type of the value) defined in a Java class. */
public class ReflectionMetaRecord extends MetaRecord
{
  protected MetaAccessor[] accessors;

  /**
   * @param clazz
   */
  public ReflectionMetaRecord(Class<?> clazz)
  {
    super(clazz);

    TreeMap<String, MetaAccessor> mas = new TreeMap<String, MetaAccessor>();

    //
    // Get the public fields
    //
    Field[] fs = clazz.getFields();
    for (int i = 0; i < fs.length; i++)
    {
      Field f = fs[i];
      String name = f.getName();
      mas.put(name, MetaField.make(name, f));
    }

    //
    // Get the public get/set methods
    //
    Method[] methods = clazz.getMethods();
    TreeMap<String, Method> getters = new TreeMap<String, Method>();
    for (int i = 0; i < methods.length; i++)
    {
      Method getter = methods[i];
      String name = getter.getName();
      Class<?>[] ps = getter.getParameterTypes();
      if (ps.length == 0 && name.startsWith("get"))
      {
        name = name.substring(3);
        getters.put(name, getter);
      }
    }
    for (int i = 0; i < methods.length; i++)
    {
      Method setter = methods[i];
      String name = setter.getName();
      Class<?>[] ps = setter.getParameterTypes();
      if (ps.length == 1 && name.startsWith("set"))
      {
        name = name.substring(3);
        Method getter = getters.get(name);
        if (getter != null && getter.getReturnType() == ps[0])
        {
          if (mas.get(name) == null) // ignore getter/setter when field is public
          {
            mas.put(name, MetaGetter.make(name, getter, setter));
          }
        }
      }
    }

    //
    // Build the accessors
    //

    int n = mas.size();
    accessors = new MetaAccessor[n];
    int i = 0;
    for (Map.Entry<String, MetaAccessor> entry : mas.entrySet())
    {
      accessors[i++] = entry.getValue();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaValue#newInstance()
   */
  @Override
  public Object newInstance()
  {
    try
    {
      return clazz.newInstance();
    }
    catch (InstantiationException e)
    {
      throw new UndeclaredThrowableException(e);
    }
    catch (IllegalAccessException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaRecord#arity()
   */
  @Override
  public int arity()
  {
    return accessors.length;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaRecord#makeItems()
   */
  @Override
  public JsonValue[] makeValues()
  {
    int n = arity();
    JsonValue[] values = new JsonValue[n];
    for (int i = 0; i < n; i++)
    {
      values[i] = accessors[i].makeValue();
    }
    return values;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaRecord#findField(com.ibm.jaql.json.type.JString)
   */
  @Override
  public int findField(JsonString name)
  {
    for (int i = 0; i < accessors.length; i++) // TODO: take advantage of accessors sorted by name
    {
      if (accessors[i].getName().equals(name))
      {
        return i;
      }
    }
    return -1;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaRecord#findField(java.lang.String)
   */
  @Override
  public int findField(String name)
  {
    for (int i = 0; i < accessors.length; i++) // TODO: take advantage of accessors sorted by name
    {
      if (accessors[i].getName().toString().equals(name))
      {
        return i;
      }
    }
    return -1;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaRecord#getName(int)
   */
  @Override
  public JsonString getName(int fieldId)
  {
    return accessors[fieldId].getName();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaRecord#getValue(java.lang.Object, int,
   *      com.ibm.jaql.json.type.Item)
   */
  @Override
  public JsonValue getValue(Object obj, int fieldId, JsonValue target)
  {
    try
    {
      MetaAccessor ma = accessors[fieldId];
      return ma.get(obj, target);
    }
    catch (IllegalArgumentException e)
    {
      throw new UndeclaredThrowableException(e);
    }
    catch (IllegalAccessException e)
    {
      throw new UndeclaredThrowableException(e);
    }
    catch (InvocationTargetException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaValue#read(java.io.DataInput,
   *      java.lang.Object)
   */
  @Override
  public Object read(DataInput in, Object obj) throws IOException
  {
    try
    {
      if (obj == null)
      {
        obj = newInstance();
      }
      for (int i = 0; i < accessors.length; i++)
      {
        accessors[i].read(in, obj);
      }
      return obj;
    }
    catch (IllegalArgumentException e)
    {
      throw new UndeclaredThrowableException(e);
    }
    catch (IllegalAccessException e)
    {
      throw new UndeclaredThrowableException(e);
    }
    catch (InvocationTargetException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaValue#write(java.io.DataOutput,
   *      java.lang.Object)
   */
  @Override
  public void write(DataOutput out, Object obj) throws IOException
  {
    try
    {
      for (int i = 0; i < accessors.length; i++)
      {
        MetaAccessor ma = accessors[i];
        ma.write(out, obj);
      }
    }
    catch (IllegalArgumentException e)
    {
      throw new UndeclaredThrowableException(e);
    }
    catch (IllegalAccessException e)
    {
      throw new UndeclaredThrowableException(e);
    }
    catch (InvocationTargetException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.meta.MetaRecord#copy(java.lang.Object,
   *      java.lang.Object)
   */
  @Override
  public void copy(Object toValue, Object fromValue)
  {
    try
    {
      for (int i = 0; i < accessors.length; i++)
      {
        MetaAccessor ma = accessors[i];
        ma.copy(toValue, fromValue);
      }
    }
    catch (IllegalArgumentException e)
    {
      throw new UndeclaredThrowableException(e);
    }
    catch (IllegalAccessException e)
    {
      throw new UndeclaredThrowableException(e);
    }
    catch (InvocationTargetException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

}
