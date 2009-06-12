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
import com.ibm.jaql.lang.util.ItemHashtable;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
public class GroupByExpr extends IterExpr
{
  /**
   * Note: the byBindings are at bindings.get(0), but moved to
   * exprs[exprs.length-2].
   * 
   * @param bindings
   * @param doExpr
   * @return
   */
  private static Expr[] makeExprs(ArrayList<BindingExpr> bindings, Expr doExpr)
  {
    int n = bindings.size();
    Expr[] exprs = new Expr[n + 1];
    for (int i = 1; i < n; i++)
    {
      BindingExpr b = bindings.get(i);
      assert b.type == BindingExpr.Type.IN;
      exprs[i - 1] = b;
    }
    BindingExpr byBinding = bindings.get(0);
    assert byBinding.type == BindingExpr.Type.EQ;
    exprs[n - 1] = byBinding;
    exprs[n] = doExpr;
    return exprs;
  }

  /**
   * @param exprs
   */
  public GroupByExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * exprs = (groupInBinding)+ groupByBinding doExpr
   * 
   * groupInByExpr is a BindingExpr, b: group b.var in b.expr[0] by b.var2 =
   * b.expr[1]
   * 
   * @param bindings
   * @param doExpr
   */
  public GroupByExpr(ArrayList<BindingExpr> bindings, Expr doExpr)
  {
    super(makeExprs(bindings, doExpr));
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
   * @return
   */
  public final int numInputs()
  {
    return exprs.length - 2;
  }

  /**
   * @param i
   * @return
   */
  public final BindingExpr inBinding(int i)
  {
    assert i < exprs.length - 2;
    return (BindingExpr) exprs[i];
  }

  /**
   * @return
   */
  public final BindingExpr byBinding()
  {
    return (BindingExpr) exprs[exprs.length - 2];
  }

  /**
   * @return
   */
  public final Expr collectExpr()
  {
    return exprs[exprs.length - 1];
  }

  /**
   * @return
   */
  public final Var byVar()
  {
    return byBinding().var;
  }

  /**
   * @param i
   * @return
   */
  public final Var getIntoVar(int i)
  {
    return inBinding(i).var2;
  }

  /**
   * @param var
   * @return
   */
  public int getIntoIndex(Var var)
  {
    int n = exprs.length - 2;
    for (int i = 0; i < n; i++)
    {
      BindingExpr b = inBinding(i);
      if (b.var2 == var)
      {
        return i;
      }
    }
    return -1;
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
    exprText.print("\ngroup( ");
    int n = exprs.length - 2;
    BindingExpr byBinding = byBinding();
    String sep = "";
    for (int i = 0; i < n; i++)
    {
      exprText.print(sep);
      BindingExpr b = inBinding(i);
      exprText.print(b.var.name);
      exprText.print(" in ");
      b.inExpr().decompile(exprText, capturedVars);
      exprText.print(" by ");
      exprText.println(byBinding.var.name);
      exprText.print(" = ");
      byBinding.byExpr(i).decompile(exprText, capturedVars);
      exprText.print(" into ");
      exprText.print(b.var2.name);
      sep = ",       ";
    }
    exprText.println(")");
    collectExpr().decompile(exprText, capturedVars);

    for (int i = 0; i < n; i++)
    {
      BindingExpr b = inBinding(i);
      capturedVars.remove(b.var);
      capturedVars.remove(b.var2);
    }
    capturedVars.remove(byBinding.var);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    // TODO: the ItemHashtable is a real quick and dirty prototype.  We need to spill to disk, etc...
    final int n = exprs.length - 2;
    final BindingExpr byBinding = byBinding();

    ItemHashtable temp = new ItemHashtable(n);

    for (int i = 0; i < n; i++)
    {
      BindingExpr b = inBinding(i);
      Item item;
      Iter iter = b.inExpr().iter(context);
      while ((item = iter.next()) != null)
      {
        context.setVar(b.var, item);
        Item byItem = byBinding.byExpr(i).eval(context);
        temp.add(i, byItem, item);
      }
    }

    final ItemHashtable.Iterator tempIter = temp.iter();

    return new Iter() {
      Iter collectIter = Iter.empty;

      public Item next() throws Exception
      {
        while (true)
        {
          Item item = collectIter.next();
          if (item != null)
          {
            return item;
          }

          if (!tempIter.next())
          {
            return null;
          }

          context.setVar(byBinding.var, tempIter.key());

          for (int i = 0; i < n; i++)
          {
            BindingExpr b = inBinding(i);
            context.setVar(b.var2, tempIter.values(i));
          }

          collectIter = exprs[n + 1].iter(context);
        }
      }
    };
  }

}
