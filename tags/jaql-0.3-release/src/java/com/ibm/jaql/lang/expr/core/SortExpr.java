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

import org.apache.hadoop.io.WritableComparator;

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.AscDescItemComparator;
import com.ibm.jaql.json.util.ItemComparator;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.json.util.ReverseItemComparator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.util.ItemSorter;

/**
 * 
 */
public class SortExpr extends IterExpr
{
  /**
   * @param input
   * @param order
   * @return
   */
  private static Expr[] makeExprs(BindingExpr input, ArrayList<OrderExpr> order)
  {
    Expr[] exprs = new Expr[order.size() + 1];
    exprs[0] = input;
    for (int i = 1; i < exprs.length; i++)
    {
      exprs[i] = order.get(i - 1);
    }
    return exprs;
  }

  /**
   * @param exprs
   */
  public SortExpr(Expr[] exprs)
  {
    super(exprs);
  }

  // exprs[0] is a BindingExpr b
  // exprs[1:*] are OrderExpr o
  // sort b.var in b.expr[0] by (o.expr[0] o.order, ...)
  public SortExpr(BindingExpr input, ArrayList<OrderExpr> order)
  {
    super(makeExprs(input, order));
  }

  //  public SortExpr(Env env, String varName, Expr inputExpr)
  //  {
  //    this.input = new Binding(Binding.IN_BINDING, env.scope(varName), inputExpr);
  //  }
  //
  //  public void addBy(Expr by, boolean asc1)
  //  {
  //    byExprs.add(by);
  //    boolean[] a = new boolean[asc.length + 1];
  //    System.arraycopy(asc, 0, a, 0, asc.length);
  //    a[asc.length] = asc1;
  //    asc = a;
  //  }
  //
  //  public void done(Env env)
  //  {
  //    env.unscope(input.var);
  //  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("\nsort( ");
    BindingExpr b = (BindingExpr) exprs[0];
    exprText.print(b.var.name);
    exprText.print(" in ");
    b.inExpr().decompile(exprText, capturedVars);
    exprText.print("\nby ");
    String sep = " ";
    for (int i = 1; i < exprs.length; i++)
    {
      OrderExpr o = (OrderExpr) exprs[i];
      exprText.print(sep);
      o.orderExpr().decompile(exprText, capturedVars);
      String order;
      switch (o.order)
      {
        case ASC :
          order = " asc";
          break;
        case DESC :
          order = " desc";
          break;
        default :
          throw new RuntimeException("illegal sort order");
      }
      exprText.print(order);
      sep = ", ";
    }
    exprText.println(")");

    capturedVars.remove(b.var);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    FixedJArray byArray = null;
    Item byItem = null;
    final int nby = exprs.length - 1;
    WritableComparator comparator;
    if (nby == 1)
    {
      OrderExpr o = (OrderExpr) exprs[1];
      if (o.order == OrderExpr.Order.ASC)
      {
        comparator = new ItemComparator();
      }
      else
      {
        comparator = new ReverseItemComparator();
      }
    }
    else
    // if( nby > 1 )
    {
      boolean[] order = new boolean[nby];
      for (int i = 1; i < exprs.length; i++)
      {
        OrderExpr o = (OrderExpr) exprs[i];
        order[i - 1] = (o.order == OrderExpr.Order.ASC);
      }
      byArray = new FixedJArray(exprs.length - 1); // TODO: memory
      byItem = new Item(byArray); // TODO: memory 
      comparator = new AscDescItemComparator(order);
    }

    final ItemSorter temp = new ItemSorter(comparator);

    BindingExpr b = (BindingExpr) exprs[0];
    Item item;
    Iter iter = b.inExpr().iter(context);
    if (iter.isNull())
    {
      return Iter.nil;
    }
    while ((item = iter.next()) != null)
    {
      context.setVar(b.var, item);
      if (nby == 1)
      {
        OrderExpr o = (OrderExpr) exprs[1];
        byItem = o.orderExpr().eval(context);
      }
      else
      {
        for (int i = 1; i < exprs.length; i++)
        {
          OrderExpr o = (OrderExpr) exprs[i];
          Item col = o.orderExpr().eval(context);
          byArray.set(i - 1, col);
        }
      }
      temp.add(byItem, item);
    }

    temp.sort();

    final Item[] byItems = new Item[nby];
    for (int i = 0; i < nby; i++)
    {
      byItems[i] = new Item();
    }

    return new Iter() {
      public Item next() throws Exception
      {
        return temp.nextValue();
      }
    };
  }
}
