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
package com.ibm.jaql.lang.expr.core;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.agg.PushAgg;
import com.ibm.jaql.lang.expr.agg.PushAggExpr;

/**
 * reduce ($i in e0 into $a1 = f1(e1<$i>), ..., $an = fn(en<$i>)) er<$a1,...$an>
 * ==> let( $I = e0, $a1 = f1( for( $i in $I ) e1<$i> ), ... $an = fn( for( $i
 * in $I ) en<$i> ) ) er<$a1,...,$an>
 */
public final class ReduceExpr extends Expr
{
  /**
   * BindingExpr in, BindingExpr agg1, ..., BindingExpr aggN, Expr ret
   * 
   * @param exprs
   */
  public ReduceExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param inVar
   * @param inExpr
   * @param aggs
   * @param retExpr
   * @return
   */
  private static Expr[] makeArgs(Var inVar, Expr inExpr,
      ArrayList<BindingExpr> aggs, Expr retExpr)
  {
    int n = aggs.size();
    Expr[] args = new Expr[n + 2];
    args[0] = new BindingExpr(BindingExpr.Type.IN, inVar, null, inExpr);
    for (int i = 0; i < n; i++)
    {
      BindingExpr b = aggs.get(i);
      if (!(b.inExpr() instanceof PushAggExpr))
      {
        throw new RuntimeException(
            "reduce expression only supports PushAggs right now...");
      }
      args[i + 1] = b;
    }
    args[n + 1] = retExpr;
    return args;
  }

  /**
   * @param inVar
   * @param inExpr
   * @param aggs
   * @param retExpr
   */
  public ReduceExpr(Var inVar, Expr inExpr, ArrayList<BindingExpr> aggs,
      Expr retExpr)
  {
    super(makeArgs(inVar, inExpr, aggs, retExpr));
  }

  /**
   * @return
   */
  public BindingExpr inBinding()
  {
    return (BindingExpr) exprs[0];
  }

  /**
   * @return
   */
  public int numAggs()
  {
    return exprs.length - 2;
  }

  /**
   * @param i
   * @return
   */
  public BindingExpr aggBinding(int i)
  {
    return (BindingExpr) exprs[i + 1];
  }

  /**
   * @return
   */
  public final Expr returnExpr()
  {
    return exprs[exprs.length - 1];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("reduce( ");
    exprText.print(inBinding().var.name);
    exprText.print(" in ");
    inBinding().inExpr().decompile(exprText, capturedVars);
    exprText.println("\ninto");
    String sep = "";
    int n = numAggs();
    for (int i = 0; i < n; i++)
    {
      exprText.println(sep);
      exprText.print(aggBinding(i).var.name);
      exprText.print(" = ");
      aggBinding(i).aggExpr().decompile(exprText, capturedVars);
      sep = ",";
    }
    exprText.println(")");
    returnExpr().decompile(exprText, capturedVars);

    capturedVars.remove(inBinding().var);
    for (int i = 0; i < n; i++)
    {
      capturedVars.remove(aggBinding(i).var);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    BindingExpr b = inBinding();
    Iter iter = b.inExpr().iter(context);
    if (iter.isNull())
    {
      return Item.nil;
    }

    final int n = numAggs();
    PushAgg[] aggs = new PushAgg[n]; // TODO: memory
    for (int i = 0; i < n; i++)
    {
      aggs[i] = aggBinding(i).aggExpr().init(context); // TODO: memory
    }

    Item item;
    while ((item = iter.next()) != null)
    {
      b.var.set(item);
      for (int i = 0; i < n; i++)
      {
        aggs[i].addMore();
      }
    }

    for (int i = 0; i < n; i++)
    {
      item = aggs[i].eval();
      aggBinding(i).var.set(item);
    }

    item = returnExpr().eval(context);
    return item;
  }

}
