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
package com.ibm.jaql.lang.expr.function;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.util.Bool3;

/** Wrapper for functions implemented in Java. */
// TODO: there is significant room for improvement (e.g., automatic conversion of types)
public class JavaFunctionCallExpr extends Expr
{
  private Object      instance;               // FIXME: this doesn't work in recursion
  private Method      method;
  private Class<?>[]  paramTypes;
  private Object[]    args;
  private SpilledJsonArray resultArray;            // FIXME: this doesn't work in recursion

  /**
   * java("com.acme.Split", ...) --> new com.acme.Split().eval(...)
   * 
   * @param fnName
   * @param cls
   * @param method
   * @param exprs
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public JavaFunctionCallExpr(Class<?> cls, Method method, Expr[] exprs)
    throws InstantiationException,
      IllegalAccessException
  {
    super(exprs);
    setup(cls, method);
  }
	
  
  /** Uses the eval(...) method with exprList.size() arguments, if existant and unique. 
   * Otherwise, throws RuntimeException
   *  
   * @param fnName
   * @param cls
   * @param exprList
   */
  public JavaFunctionCallExpr(Class<?> cls, List<Expr> exprList)
  {
    super(exprList);
    try
    {
      Method[] methods = cls.getMethods();
      for (Method m : methods)
      {
        String n = m.getName();
        if ("eval".equals(n))
        {
          Class<?>[] p = m.getParameterTypes();
          if (p.length == exprs.length)
          {
            if (method != null)
            {
              throw new RuntimeException("ambiguous eval methods on class "
                  + cls.getName());
            }

            method = m;
          }
        }
      }
      if (method == null)
      {
        throw new RuntimeException("no eval method on class " + cls.getName()
            + " with " + exprs.length + " arguments");
      }
      setup(cls, method);
    }
    catch (Exception ex)
    {
      throw new UndeclaredThrowableException(ex);
    }
  }

  /**
   * @param fnName
   * @param cls
   * @param method
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  private void setup(Class<?> cls, Method method)
      throws InstantiationException, IllegalAccessException
  {
    this.instance = cls.newInstance();
    this.method = method;
    this.paramTypes = method.getParameterTypes();
    int n = paramTypes.length;
    if (n != exprs.length)
    {
      throw new RuntimeException(
          "java method does not have the right number of arguments: "
              + cls.getName());
    }
    for (int i = 0; i < n; i++)
    {
      Class<?> p = paramTypes[i];
      if (!(JsonValue.class.isAssignableFrom(p) || JsonIterator.class.isAssignableFrom(p)))
      {
        throw new RuntimeException("illegal argument to java method: "
            + cls.getName() + ": " + p.getName());
      }
      // TODO: we can add some typechecking here when we have type inference
    }
    Class<?> rt = method.getReturnType();
    if (JsonValue.class.isAssignableFrom(rt))
    {
    }
    else if (JsonIterator.class.isAssignableFrom(rt))
    {
    }
    else
    {
      throw new RuntimeException("illegal return type from java method: "
          + cls.getName() + ": " + rt.getName());
    }
    args = new Object[n];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print(kw("builtin") + "('com.ibm.jaql.lang.expr.function.JavaUdfExpr$Descriptor')('" + instance.getClass().getName() + "')");
    String sep = "( ";
    for (Expr e : exprs)
    {
      exprText.print(sep);
      e.decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print(" )");
  }

  /** 
   * This expression evaluates all input arguments only once
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }


  public Map<ExprProperty, Boolean> getProperties()
  {
    return ExprProperty.createSafeDefaults();
  }

	@SuppressWarnings("unchecked")
  @Override
  public Schema getSchema()
  {
    Class<?> c =  method.getReturnType();
    //Handle JsonIterator as special case because it is no in the type hierarchy of JsonValue
    if (JsonIterator.class.isAssignableFrom(c))
    {
      return SchemaFactory.arrayOrNullSchema();
    }
    
    if(c.equals(JsonValue.class)) {
    	return SchemaFactory.anySchema();
    }
    
    return SchemaFactory.make((Class<? extends JsonValue>)c);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  @Override
  public Expr clone(VarMap varMap)
  {
    try
    {
      return new JavaFunctionCallExpr(instance.getClass(), method,
          cloneChildren(varMap));
    }
    catch (Exception ex)
    {
      throw new UndeclaredThrowableException(ex);
    }
  }

  /**
   * @param context
   * @return
   * @throws Exception
   */
  private Object makeCall(Context context) throws Exception
  {
    int n = paramTypes.length;
    for (int i = 0; i < n; i++)
    {
      Class<?> p = paramTypes[i];
      if (JsonValue.class.isAssignableFrom(p))
      {
        args[i] = exprs[i].eval(context);
      }
      else if (JsonIterator.class.isAssignableFrom(p))
      {
        JsonIterator iter = exprs[i].iter(context);
        if (iter.isNull())
        {
          args[i] = null;
        }
        else
        {
          args[i] = iter;
        }
      }
      else
      {
        throw new RuntimeException(
            "java method has unsupported type! this should have been caught earlier...");
      }
    }
    Object result = method.invoke(instance, args);
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    Object result = makeCall(context);
    if (result == null)
    {
      return null;
    }
    else if (result instanceof JsonValue)
    {
      return (JsonValue)result;
    }
    else if (result instanceof JsonIterator)
    {
      JsonIterator iter = (JsonIterator) result;
      if (iter.isNull())
      {
        return null;
      }
      if (resultArray == null)
      {
        resultArray = new SpilledJsonArray();
      }
      resultArray.setCopy(iter);
      return resultArray;
    }
    else
    {
      throw new RuntimeException(
          "java method returns unsupported type! this should have been caught earlier...");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(Context context) throws Exception
  {
    Object result = makeCall(context);

    if (result == null)
    {
      return JsonIterator.NULL;
    }
    else if (result instanceof JsonIterator)
    {
      return (JsonIterator) result;
    }
    else
    {
      JsonArray arr = (JsonArray) result; // cast error possible
      return arr.iter();
    }
  }
}
