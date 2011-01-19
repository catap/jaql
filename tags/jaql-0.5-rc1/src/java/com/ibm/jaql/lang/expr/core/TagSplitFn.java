/*
 * Copyright (C) IBM Corp. 2010.
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

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.util.Bool3;

/**
 * This function is used internally during the rewriting of tee().  
 * It is not intended for general use.
 * 
 * e -> tagSplit( f0, ..., fn )
 * 
 * Exactly the same as:
 *   ( X = e, 
 *     X -> filter $[0] == 0 -> f0() -> transform $[1], ...
 *     X -> filter $[0] == n -> fn() -> transform $[1] )
 *     
 * Also the same as:
 *   ( e -> write( composite( [t0, ..., tn] ) ),
 *     read(t0) -> f0(), ...
 *     read(tn) -> fn() )
 */
public class TagSplitFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par2u
  {
    public Descriptor()
    {
      super("tagSplit", TagSplitFn.class);
    }
  }
  
  public TagSplitFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.valueOf(i == 0);
  }
  

// TODO:
//  @Override
//  public Schema getSchema()

  
  protected Function evalUntilLast(final Context context) throws Exception
  {
    final Function[] funcs = new Function[exprs.length-1];
    SpilledJsonArray[] inputs = new SpilledJsonArray[funcs.length];
    for(int i = 0 ; i < funcs.length ; i++)
    {
      funcs[i] = (Function)exprs[i+1].eval(context);
      inputs[i] = new SpilledJsonArray();
    }
    
    // Partition the input to each function
    JsonIterator iter = exprs[0].iter(context);
    JsonValue[] pair = new JsonValue[2];
    for( JsonValue val: iter )
    {
      JsonArray apair = (JsonArray)val;
      apair.getAll(pair);
      int i = ((JsonNumber)pair[0]).intValueExact();
      inputs[i].addCopy(pair[1]);
    }
    
    // Evaluate each function over its partition
    // The last must return an array, which is returned by this function.
    int n = funcs.length - 1;
    for(int i = 0 ; i < n ; i++)
    {
      funcs[i].setArguments(inputs[i]);
      funcs[i].eval(context);
    }
    funcs[n].setArguments(inputs[n]);
    return funcs[n];
  }

  @Override
  public JsonValue eval(final Context context) throws Exception
  {
    return evalUntilLast(context).eval(context);
  }

  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    return evalUntilLast(context).iter(context);
  }

  /**
   * Replace this function by its definition:
   * 
   *    tagSplit( e, f0, ..., fn )
   * ==>
   *   ( fd = e -> write( composite( [t0, ..., tn] ) ),
   *     read(fd.descriptors[0]) -> f0(), ...
   *     read(fd.descriptors[n]) -> fn() )
   */
//  public Expr expand(Env env)
//  {
//    Var fdVar = env.makeVar("$splitFd");
//    Expr[] fds = new Expr[exprs.length-1];
//    Expr[] legs = new Expr[exprs.length];
//    for(int i = 1 ; i < legs.length ; i++)
//    {
//      Expr fnExpr = exprs[i];
//      JaqlFunction fn = DefineJaqlFunctionExpr.getPartialFunction(fnExpr);
//      if( fn != null )
//      {
//        if( ! fn.canBeCalledWith(1) )
//        {
//          throw new RuntimeException(fn.formatError("must be callable with one argument"));
//        }
//        Var fnArg = fn.getParameters().get(0).getVar();
//        Expr body = fn.body();
//        if( body.f)
//      }
//      Expr fncall = new FunctionCallExpr(fn, new ArrayExpr(new VarExpr(forVar)));
//      Var tvar = env.makeVar("$");
//      legs[i] = new TransformExpr(tvar, fncall, new ArrayExpr(new ConstExpr(i), new VarExpr(tvar)));
//    }
//    Expr expr = new MergeFn(legs);
//    expr = new ForExpr(forVar, exprs[0], expr);
//    if( parent != null )
//    {
//      replaceInParent(expr);
//    }
//    return expr;
//  }
}
