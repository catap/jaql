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

import java.util.HashSet;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;

/** 
 * A built-in function value. Built-in functions are implemented using {@link Expr}s, i.e.,
 * they integrate into Jaql's AST. 
 */
public final class BuiltInFunction extends Function
{
  private static final Expr[] NO_ARGS = new Expr[0];
 
  /** Data structure that holds arguments for evaluation/inlining */
  private transient Expr[] args = NO_ARGS;
  
  /** Descriptor of the function */
  private BuiltInFunctionDescriptor descriptor;

  
  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs from the specified function descriptor. */
  public BuiltInFunction(BuiltInFunctionDescriptor descriptor)
  {
    if (descriptor == null)
    {
      throw new IllegalArgumentException("descriptor must not be null");
    }
    this.descriptor = descriptor;
  }
  
  
  // -- self-description --------------------------------------------------------------------------
  
  /** Returns the descriptor of this function. */
  public BuiltInFunctionDescriptor getDescriptor()
  {
    return descriptor;
  }
  
  @Override
  public JsonValueParameters getParameters()
  {
    return getDescriptor().getParameters();  
  }
  
  @Override
  public String formatError(String msg)
  {
    return "In call of builtin function " + descriptor.getName() + ": " + msg;
  }

  /** Tries to return a descriptor for the function implemented by the specified class.
   * To do so, checks whether the class has an inner class the implements 
   * {@link BuiltInFunctionDescriptor} and, if so, returns an instance of this class.
   * 
   * @throws IllegalArgumentException if no unique, instantiable descriptor has been found
   */
  public static BuiltInFunctionDescriptor getDescriptor(Class<? extends Expr> cls) 
  {
    BuiltInFunctionDescriptor descriptor = null;
    Class<?>[] inners = cls.getDeclaredClasses();
    for (int i = 0; i < inners.length; i++)
    {
      Class<?> c = inners[i];
      if (BuiltInFunctionDescriptor.class.isAssignableFrom(c))
      {
        if (descriptor != null)
        {
          throw new IllegalArgumentException(cls + " contains two descriptor classes");
        }
        try
        {
          descriptor = (BuiltInFunctionDescriptor) c.newInstance();
        } catch (InstantiationException e)
        {
          // ignore
        } catch (IllegalAccessException e)
        {
          // ignore
        }
      }
    }
    if (descriptor == null)
    {
      throw new IllegalArgumentException(cls + " does not have an inner descriptor class");
    }
    return descriptor;
  }
  
  
  // -- evaluation / inlining ---------------------------------------------------------------------

  @Override
  protected void prepare(int numArgs)
  {
    if (args.length != numArgs)
    {
      args = new Expr[numArgs];
    }
  }

  @Override
  protected void setArgument(int pos, JsonValue value)
  {
    args[pos] = new ConstExpr(value);
  }

  @Override
  protected void setArgument(int pos, JsonIterator it)
  {
    // TODO avoid copying when possible
    try 
    {
      SpilledJsonArray a = new SpilledJsonArray();
      a.addCopyAll(it);
      args[pos] = new ConstExpr(a);
    }
    catch (Exception e)
    {
      throw JaqlUtil.rethrow(e);
    }
  }

  @Override
  protected void setArgument(int pos, Expr expr)
  {
    args[pos] = expr;
  }

  @Override
  protected void setDefault(int pos)
  {
    setArgument(pos, getParameters().defaultOf(pos));
  }
  
  @Override
  public Expr inline(boolean forEval)
  {
    if (forEval)
    {
      // cloning necessary because object construction changes parent field in expr's
      Expr[] clonedArgs = new Expr[args.length];
      VarMap varMap = new VarMap();
      for (int i=0; i<args.length; i++)
      {
        HashSet<Var> vars = args[i].getCapturedVars();
        for (Var v : vars) 
        {
          varMap.put(v, v);
        }
        clonedArgs[i] = args[i].clone(varMap);
      }
      return descriptor.construct(clonedArgs);
    }
    return descriptor.construct(args);
  }

  /** Returns the arguments of this function. The arguments have to be set using one of the 
   * <code>setArguments</code> functions. */
  public Expr[] getArguments()
  {
    return args;
  }
  
  // -- copying ----------------------------------------------------------------------------------- 
  
  @Override
  public BuiltInFunction getCopy(JsonValue target)
  {
    return new BuiltInFunction(descriptor);
  }

  @Override
  public Function getImmutableCopy()
  {
    return new BuiltInFunction(descriptor);
  }
}
