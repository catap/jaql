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
package com.ibm.jaql.json.constructor;

import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JValue;

/**
 * 
 */
public class JConstructor
{
  private static HashMap<String, ArrayList<JMethod>> lib = new HashMap<String, ArrayList<JMethod>>();

  static
  {
    add(new JMethod("date", DateConstructors.class, "date", 1));
    add(new JMethod("span", SpanConstructors.class, "span", 2));
  }

  /**
   * @param fn
   */
  public static void add(JMethod fn)
  {
    ArrayList<JMethod> am = lib.get(fn.getName());
    if (am == null)
    {
      am = new ArrayList<JMethod>();
      lib.put(fn.getName(), am);
    }
    int a = fn.arity();
    am.ensureCapacity(a);
    for (int i = am.size(); i <= a; i++)
    {
      am.add(null);
    }
    am.set(a, fn);
  }

  /**
   * @param fnName
   * @param arity
   * @return
   */
  public static JMethod get(String fnName, int arity)
  {
    ArrayList<JMethod> am = lib.get(fnName);
    if (am == null)
    {
      throw new RuntimeException("no registered constructor named " + fnName);
    }
    JMethod m = am.get(arity);
    if (m == null)
    {
      throw new RuntimeException("constructor " + fnName + " does not take "
          + arity + " arguments");
    }
    return m;
  }

  /**
   * @param fnName
   * @param args
   * @return
   */
  public static Item eval(String fnName, Item[] args)
  {
    JMethod fn = get(fnName, args.length);
    JValue[] jargs = new JValue[args.length];
    for (int i = 0; i < args.length; i++)
    {
      jargs[i] = args[i].get();
    }
    JValue result = fn.eval(jargs);
    return new Item(result);
  }
}

/**
 * 
 */
class JMethod // TODO: unify with Jaql UDFs
{
  private String     fnName;
  private Object     instance;
  private Method     method;
  private Class<?>[] paramTypes;

  /**
   * @param fnName
   * @param cls
   * @param methodName
   * @param numArgs
   */
  public JMethod(String fnName, Class<?> cls, String methodName, int numArgs)
  {
    this.fnName = fnName;
    Method[] methods = cls.getMethods();
    for (Method m : methods)
    {
      String n = m.getName();
      if (methodName.equals(n))
      {
        Class<?>[] p = m.getParameterTypes();
        if (p.length == numArgs)
        {
          if (method != null)
          {
            throw new RuntimeException("ambiguous eval methods on class "
                + cls.getName());
          }
          method = m;
          paramTypes = p;
        }
      }
    }
    if (method == null)
    {
      throw new RuntimeException("no " + methodName + " method on class "
          + cls.getName() + " with " + numArgs + " arguments");
    }

    try
    {
      this.instance = cls.newInstance();
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

  /**
   * @return
   */
  public int arity()
  {
    return paramTypes.length;
  }

  /**
   * @return
   */
  public String getName()
  {
    return fnName;
  }

  /**
   * @param args
   * @return
   */
  public JValue eval(JValue[] args)
  {
    try
    {
      JValue result = (JValue) method.invoke(instance, (Object[]) args);
      return result;
    }
    catch (Exception ex)
    {
      throw new UndeclaredThrowableException(ex);
    }
  }
}
