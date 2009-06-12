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
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.json.util.ScalarIter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.util.ItemHashtable;

/**
 * 
 */
public class JoinExpr extends IterExpr
{
  /**
   * @param bindings
   * @param collectExpr
   * @return
   */
  private static Expr[] makeExprs(ArrayList<BindingExpr> bindings,
      Expr collectExpr)
  {
    int n = bindings.size();
    Expr[] exprs = new Expr[n + 1];
    for (int i = 0; i < n; i++)
    {
      BindingExpr b = bindings.get(i);
      assert b.type == BindingExpr.Type.IN;
      exprs[i] = b;
    }
    exprs[n] = collectExpr;
    return exprs;
  }

  /**
   * @param exprs
   */
  public JoinExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * exprs = (bindingExpr)+ returnExpr
   * 
   * @param bindings
   * @param returnExpr
   */
  public JoinExpr(ArrayList<BindingExpr> bindings, Expr returnExpr)
  {
    super(makeExprs(bindings, returnExpr));
  }

  /**
   * @return
   */
  public int numBindings()
  {
    return exprs.length - 1;
  }

  /**
   * @param i
   * @return
   */
  public BindingExpr binding(int i)
  {
    return (BindingExpr) exprs[i];
  }

  /**
   * @return
   */
  public Expr collectExpr()
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
    exprText.print("\njoin( ");
    int n = exprs.length - 1;
    String sep = "";
    for (int i = 0; i < n; i++)
    {
      exprText.print(sep);
      BindingExpr b = binding(i);
      if (b.optional)
      {
        exprText.print("optional ");
      }
      exprText.print(b.var.name);
      exprText.print(" in ");
      b.inExpr().decompile(exprText, capturedVars);
      exprText.print(" on ");
      b.onExpr().decompile(exprText, capturedVars);
      sep = ",     ";
    }
    exprText.println(")");
    collectExpr().decompile(exprText, capturedVars);

    for (int i = 0; i < n; i++)
    {
      BindingExpr b = binding(i);
      capturedVars.remove(b.var);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    // TODO: the ItemHashtable is a real quick and dirty prototype.  We need to spill to disk, etc...
    final int n = exprs.length - 1;
    ItemHashtable temp = new ItemHashtable(n);
    final ScalarIter[] nilIters = new ScalarIter[n];

    for (int i = 0; i < n; i++)
    {
      BindingExpr b = binding(i);
      Item item;
      Iter iter = b.inExpr().iter(context);
      while ((item = iter.next()) != null)
      {
        context.setVar(b.var, item);
        Item key = b.onExpr().eval(context);
        if (!key.isNull())
        {
          temp.add(i, key, item);
        }
      }
      if (b.optional)
      {
        nilIters[i] = new ScalarIter(Item.nil);
      }
    }

    final ItemHashtable.Iterator tempIter = temp.iter();
    final Iter[] groupIters = new Iter[n];

    return new Iter() {
      int  i           = -1;
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

          do
          {
            if (i < 0)
            {
              if (!tempIter.next())
              {
                return null;
              }

              for (int i = 0; i < n; i++)
              {
                BindingExpr b = binding(i);
                item = tempIter.values(i);
                JArray arr = (JArray) item.get();
                if (arr.isEmpty() && b.optional)
                {
                  nilIters[i].reset(Item.nil);
                  groupIters[i] = nilIters[i];
                }
                else
                {
                  groupIters[i] = arr.iter(); // TODO: should be able to reuse array iterator
                }
              }

              i = 0;
            }

            BindingExpr b = binding(i);
            item = groupIters[i].next();
            if (item != null)
            {
              context.setVar(b.var, item);
              i++;
            }
            else
            {
              item = tempIter.values(i);
              JArray arr = (JArray) item.get();
              if (arr.isEmpty() && b.optional)
              {
                nilIters[i].reset(Item.nil);
                groupIters[i] = nilIters[i];
              }
              else
              {
                groupIters[i] = arr.iter(); // TODO: should be able to reuse array iterator
              }
              i--;
            }
          } while (i < n);

          i = n - 1;
          collectIter = collectExpr().iter(context);
        }
      }
    };
  }

}
