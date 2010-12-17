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

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.array.UnionFn;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.FunctionCallExpr;
import com.ibm.jaql.util.Bool3;

/**
 * This function is used internally during the rewriting of tee().  
 * It is not intended for general use.
 * 
 * e -> tagDiamond( f0, ..., fn )
 * 
 * Exactly the same as:
 *   e -> expand union( [$] -> f0() -> transform [0,$], ..., 
 *                      [$] -> fn() -> transform [n,$] )
 */
public class DiamondTagFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par2u
  {
    public Descriptor()
    {
      super("diamondTag", DiamondTagFn.class);
    }
  }
  
  public DiamondTagFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.valueOf(i == 0);
  }
  
  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
  }

// TODO:
//  @Override
//  public Schema getSchema()

  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    final Function[] funcs = new Function[exprs.length-1];
    for(int i = 0 ; i < funcs.length ; i++)
    {
      funcs[i] = (Function)exprs[i+1].eval(context);
    }
    final MutableJsonLong id = new MutableJsonLong();
    final BufferedJsonArray pair = new BufferedJsonArray(2);
    pair.set(0, id);
    final BufferedJsonArray singleton = new BufferedJsonArray(1);
    
    return new JsonIterator(pair)
    {
      JsonIterator iter = exprs[0].iter(context);
      JsonIterator inner = JsonIterator.EMPTY;
      int nextFunc = funcs.length;
      
      @Override
      protected boolean moveNextRaw() throws Exception
      {
        while( true )
        {
          if( inner.moveNext() )
          {
            JsonValue val = inner.current();
            pair.set(1, val);
            return true;
          }
          if( nextFunc >= funcs.length )
          {
            if( !iter.moveNext() )
            {
              inner = JsonIterator.EMPTY;
              return false;
            }
            nextFunc = 0;
            JsonValue val = iter.current();
            singleton.set(0, val);
          }
          id.set(nextFunc);
          funcs[nextFunc].setArguments(singleton);
          inner = funcs[nextFunc].iter(context);
          nextFunc++;
        }
      }
    };
  }
 
  /** 
   * Replace this function by its definition:
   *    tagDiamond(e, f0, ..., fn )
   *  ==> 
   *    e -> expand each $dx 
   *           union( [$dx] -> f0() -> transform [0,$], ..., 
   *                  [$dx] -> fn() -> transform [n,$] ) 
   */
  public Expr expand(Env env)
  {
    Var forVar = env.makeVar("$dx");
    Expr[] legs = new Expr[exprs.length-1];
    for(int i = 0 ; i < legs.length ; i++)
    {
      Expr fn = exprs[i+1];
      Expr fncall = new FunctionCallExpr(fn, new ArrayExpr(new VarExpr(forVar)));
      Var tvar = env.makeVar("$");
      legs[i] = new TransformExpr(tvar, fncall, new ArrayExpr(new ConstExpr(i), new VarExpr(tvar)));
    }
    Expr expr = new UnionFn(legs);
    expr = new ForExpr(forVar, exprs[0], expr);
    if( parent != null )
    {
      replaceInParent(expr);
    }
    return expr;
  }
}
