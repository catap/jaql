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

import java.util.ArrayList;

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.agg.Aggregate;


public class AggregateFullExpr extends AggregateExpr
{
  protected Aggregate[] aggs;
  
  protected static Expr[] makeArgs(BindingExpr input, ArrayList<Aggregate> aggs)
  {
    int n = aggs.size();
    Expr[] args = new Expr[n+1];
    args[0] = input;
    for(int i = 0 ; i < n ; i++)
    {
      args[i+1] = aggs.get(i);
    }
    return args;
  }
  
  private static Expr splitExpr(Var aggVar, Var outVar, Expr expr, ArrayList<Aggregate> aggs)
  {
    if( expr instanceof VarExpr )
    {
      VarExpr ve = (VarExpr)expr;
      if( ve.var == aggVar )
      {
        throw new RuntimeException("the aggregation variable must be inside an aggregate");
      }
    }
    else if( expr instanceof Aggregate )
    {
      Aggregate agg = (Aggregate)expr;
      int i = aggs.size();
      aggs.add(agg);
      Expr e = new IndexExpr(new VarExpr(outVar), i);
      if( agg.parent() == null )
      {
        return e;
      }
      else
      {
        agg.replaceInParent(e);
      }
    }
    else
    {
      for( Expr e: expr.exprs )
      {
        splitExpr(aggVar, outVar, e, aggs);
      }
    }
    return expr;
  }

  /**
   * Return a new canonical aggregate expression, which might have an MapExpr on top of it.
   * 
   * @param env
   * @param aggVar
   * @param input
   * @param expr
   * @param expand True if expanding expr.
   * @return
   */
  public static Expr make(Env env, BindingExpr input, Expr expr, boolean expand)
  {
    if( !expand && expr instanceof ArrayExpr )
    {
      // Don't add map if we are already canonical: aggregate [ agg1(..), ..., aggN(...) ]
      boolean allAggs = true;
      for( Expr e: expr.exprs )
      {
        if( ! (e instanceof Aggregate) )
        {
          allAggs = false;
          break;
        }
      }
      if( allAggs )
      {
        int n = expr.numChildren();
        Expr[] exprs = new Expr[n + 1];
        exprs[0] = input;
        System.arraycopy(expr.exprs, 0, exprs, 1, n);
        return new AggregateFullExpr(exprs);
      }
    }
    Var outVar = env.makeVar("$");
    ArrayList<Aggregate> aggs = new ArrayList<Aggregate>();
    expr = splitExpr(input.var, outVar, expr, aggs);
    Expr e = new AggregateFullExpr(input, aggs);
    if( expand )
    {
      e = new ForExpr(outVar, e, expr);
    }
    else
    {
      e = new TransformExpr(outVar, e, expr);
    }
    return e;
  }

  
  // Binding input, Aggregate[] aggregates 
  public AggregateFullExpr(Expr[] inputs)
  {
    super(inputs);
  }
  
  public AggregateFullExpr(BindingExpr binding, ArrayList<Aggregate> aggs)
  {
    super(makeArgs(binding, aggs));
  }
  
  public AggType getAggType()
  {
    return AggType.FULL;
  }

  protected void makeWorkingArea()
  {
    if( aggs == null )
    {
      super.makeWorkingArea();
      int n = numAggs();
      aggs = new Aggregate[n];
      for(int i = 0 ; i < n ; i++)
      {
        aggs[i] = agg(i);
      }
    }
    else
    {
      assert numAggs() == aggs.length && aggs.length == tempAggs.length;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    makeWorkingArea();
    boolean hadInput = evalInitial(context, aggs);
    return finalResult(hadInput, aggs);
  }
}
