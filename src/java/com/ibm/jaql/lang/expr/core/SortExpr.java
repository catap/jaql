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
import java.util.HashSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JComparator;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.util.ItemSorter;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
public class SortExpr extends IterExpr
{
  /**
   * @param exprs: Expr input, Expr cmp
   */
  public SortExpr(Expr[] exprs)
  {
    super(exprs);
  }

  // exprs[0] is a BindingExpr b
  public SortExpr(Expr input, Expr cmp)
  {
    super(input, cmp);
  }
  
  public Expr cmpExpr()
  {
    return exprs[1];
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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprs[0].decompile(exprText, capturedVars);
    exprText.print("\n  -> sort using (");
    exprs[1].decompile(exprText, capturedVars);
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    JFunction cmpFn = (JFunction)cmpExpr().eval(context).get();
    if( cmpFn.getNumParameters() != 1 || !(cmpFn.getBody() instanceof CmpExpr) )
    {
      throw new RuntimeException("invalid comparator function");
    }
    Var cmpVar = cmpFn.param(0);
    CmpExpr cmp = (CmpExpr)cmpFn.getBody();
    JComparator comparator = cmp.getComparator(context);

    final ItemSorter temp = new ItemSorter(comparator);

    Item item;
    Iter iter = exprs[0].iter(context);
    if (iter.isNull())
    {
      return Iter.nil;
    }
    while ((item = iter.next()) != null)
    {
      cmpVar.setValue(item);
      Item byItem = cmp.eval(context);
      temp.add(byItem, item);
    }

    temp.sort();

//    final Item[] byItems = new Item[nby];
//    for (int i = 0; i < nby; i++)
//    {
//      byItems[i] = new Item();
//    }

    return new Iter() {
      public Item next() throws Exception
      {
        return temp.nextValue();
      }
    };
  }

}
