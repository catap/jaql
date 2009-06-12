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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.json.util.ScalarIter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.agg.Aggregate;
import com.ibm.jaql.lang.expr.agg.AlgebraicAggregate;
import com.ibm.jaql.util.Bool3;


public final class AggregateExpr extends IterExpr // TODO: add init/combine/final flags
{
  protected static Expr[] makeArgs(Var aggVar, Expr input, ArrayList<Aggregate> aggs)
  {
    int n = aggs.size();
    Expr[] args = new Expr[n+1];
    args[0] = new BindingExpr(BindingExpr.Type.IN, aggVar, null, input);
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
   * @return
   */
  public static Expr make(Env env, Var aggVar, Expr input, Expr expr)
  {
    if( expr instanceof ArrayExpr )
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
        exprs[0] = new BindingExpr(BindingExpr.Type.IN, aggVar, null, input);
        System.arraycopy(expr.exprs, 0, exprs, 1, n);
        return new AggregateExpr(exprs);
      }
    }
    Var outVar = env.makeVar("$");
    ArrayList<Aggregate> aggs = new ArrayList<Aggregate>();
    expr = splitExpr(aggVar, outVar, expr, aggs);
    Expr e = new AggregateExpr(aggVar, input, aggs);
    e = new TransformExpr(outVar, e, expr);
    return e;
  }

  // Binding input, Aggregate[] aggregates 
  public AggregateExpr(Expr[] inputs)
  {
    super(inputs);
  }
  
  public AggregateExpr(Var aggVar, Expr input, ArrayList<Aggregate> aggs)
  {
    super(makeArgs(aggVar, input, aggs));
  }
  
  public final BindingExpr binding()
  {
    return (BindingExpr)exprs[0];
  }

  public final int numAggs()
  {
    return exprs.length - 1;
  }

  public final Aggregate agg(int i)
  {
    return (Aggregate)exprs[i+1];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
   */
  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  /**
   * 
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    if( i == 0 )
    {
      return Bool3.TRUE;
    }
    return Bool3.FALSE;
  }

  /**
   * @return true iff all aggregates are algebraic.
   */
  public boolean isAlgebraic()
  {
    for(int i = 1 ; i < exprs.length ; i++)
    {
      if( !(exprs[i] instanceof AlgebraicAggregate) )
      {
        return false;
      }
    }
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars) // TODO: get rid of captured vars during decompile
      throws Exception
  {
    // input -> aggregate (each var)? expr
    final BindingExpr in = binding();
    in.inExpr().decompile(exprText, capturedVars);
    exprText.print("\n-> aggregate each ");
    exprText.print(in.var.name());
    exprText.print(" [ ");
    String sep = "";
    int n = numAggs();
    for(int i = 0 ; i < n ; i++)
    {
      exprText.print(sep);
      Aggregate agg = agg(i);
      agg.decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print(" ]");
    capturedVars.remove(in.var);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Iter iter(final Context context) throws Exception
  {
    BindingExpr in = binding();
    final int n = numAggs();
    
    Aggregate[] aggs = new Aggregate[n]; // TODO: memory
    for(int i = 0 ; i < n ; i++)
    {
      aggs[i] = agg(i);
      aggs[i].initInitial(context);
    }

    boolean hadInput = false;
    Iter iter = in.inExpr().iter(context);
    Item item;

    while( (item = iter.next()) != null )
    {
      hadInput = true;
      context.setVar(in.var, item);
      for(int i = 0 ; i < n ; i++)
      {
        aggs[i].evalInitial(context);
      }
    }
    
    if( hadInput == false )
    {
      return Iter.empty; // TODO: Iter.nil?  preserve nil input?
    }
    
    FixedJArray tuple = new FixedJArray(n); // TODO: memory
    for(int i = 0 ; i < n ; i++)
    {
      item = aggs[i].getFinal();
      tuple.set(i, item);
    }
    item = new Item(tuple); // TODO: memory
    return new ScalarIter(item); // TODO: memory
  }

}
