/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.lang.expr.core;
import java.util.Arrays;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.FunctionCallExpr;
import com.ibm.jaql.util.Bool3;

/**
 * Calls the composition of a set of single argument functions.
 * 
 * daisyChain(T0 input, [f1, f2, ..., fn]) returns Tn
 * 
 * where:
 *   f1(T0) returns T1,
 *   f2(T1) returns T2,
 *   fn(Tn) returns Tn
 *   
 * A compose function that returns a function is easily created from this one:
 *   compose = fn(fns) fn(input) daisyChain(input, fns)
 */
public class DaisyChainFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("daisyChain", DaisyChainFn.class);
    }
  }
  
  protected LazyEvalExpr[] callers = new LazyEvalExpr[0];
  
  // input, [fns]
  public DaisyChainFn(Expr... inputs)
  {
    super(inputs);
  }
  
  public Expr inputExpr()
  {
    return exprs[0];
  }

  public Expr fnsExpr()
  {
    return exprs[1];
  }
  
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  /**
   * Attempt to expand the function calls into the expression tree.
   * This works when the function list is compile-time computable or
   * when the function list expression is a literal array:
   * 
   *   daisyChain(input, [f1,f2,...,fn]) 
   *   ==>
   *   input -> f1() -> f2() -> ... -> fn()
   * 
   * Returns true if this Expr has been replaced by the expansion.
   *         false if the tree was unmodified.
   */
  public boolean inlineIfPossible() throws Exception
  {
    Expr fe = fnsExpr();
    if( fe.isCompileTimeComputable().always() )
    {
      inlineConst();
      return true;
    }
    else if( fe instanceof ArrayExpr )
    {
      inlineList();
    }
    // we cannot expand
    return false;
  }

  /**
   * daisyChain(input, const([f1,f2,...,fn])
   * ==>
   * input -> f1() -> f2() -> ... -> fn()
   */
  protected void inlineConst() throws Exception
  {
    JsonArray fnArray = (JsonArray)fnsExpr().compileTimeEval();
    if( fnArray == null )
    {
      fnArray = JsonArray.EMPTY;
    }
    // We do all the casts before we start messing up the expr tree, just to keep the tree healthy on error.
    int n = (int)fnArray.count();
    Function[] fns = new Function[n];
    int i = 0;
    for( JsonValue v: fnArray )
    {
      fns[i++] = (Function)v;
    }
    Expr prevExpr = inputExpr();
    for( Function f: fns )
    {
      prevExpr = new FunctionCallExpr(new ConstExpr(f), prevExpr);
    }
    this.replaceInParent(prevExpr);
  }

  /**
   * daisyChain(input, [e1,e2,...,en])
   * ==>
   * input -> e1() -> e2() -> ... -> en()
   */
  protected void inlineList() throws Exception
  {
    ArrayExpr fnArray = (ArrayExpr)fnsExpr();
    Expr prevExpr = inputExpr();
    for( Expr fe: fnArray.children() )
    {
      prevExpr = new FunctionCallExpr(fe, prevExpr);
    }
    this.replaceInParent(prevExpr);
  }

  /**
   * Set the arguments to all the functions: fn(...(f2(f1(input))))
   * Return the Expr 
   */
  protected Expr composeArguments(final Context context) throws Exception
  {
    JsonArray fnArray = (JsonArray)fnsExpr().eval(context);
    if( fnArray == null )
    {
      fnArray = JsonArray.EMPTY;
    }
    int n = (int)fnArray.count();
    if( callers.length < n )
    {
      int k = callers.length;
      callers = Arrays.copyOf(callers, n);
      for( ; k < callers.length ; k++)
      {
        callers[k] = new LazyEvalExpr();
      }
    }
    n = 0;
    for( JsonValue v: fnArray )
    {
      callers[n++].fn = context.getCallable(this, (Function)v);
    }
    Expr prevExpr = inputExpr();
    for(int i = 0 ; i < n ; i++)
    {
      callers[i].fn.setArguments(prevExpr);
      prevExpr = callers[i];
    }
    return prevExpr;
  }
  
  @Override 
  protected JsonValue evalRaw(final Context context) throws Exception
  {
    return composeArguments(context).eval(context);
  }
  
  @Override 
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    return composeArguments(context).iter(context);
  }
  
  /**
   * This is a fake expression to allow our composed function to be lazy-evaluated.
   */
  protected static class LazyEvalExpr extends Expr
  {
    // The function must be callable
    protected Function fn;

    // The function must be set later
    public LazyEvalExpr()
    {
    }

    // The function must be callable
    public LazyEvalExpr(Function fn)
    {
      this.fn = fn;
    }
    
    @Override 
    protected JsonValue evalRaw(final Context context) throws Exception
    {
      return fn.eval(context);
    }
    
    @Override 
    protected JsonIterator iterRaw(final Context context) throws Exception
    {
      return fn.iter(context);
    }
  }
}
