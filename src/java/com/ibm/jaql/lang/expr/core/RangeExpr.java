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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
@JaqlFn(fnName="range", minArgs=2, maxArgs=2)
public class RangeExpr extends IterExpr
{
  /**
   * @param exprs
   */
  public RangeExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * expr0 to expr1
   * 
   * @param expr0
   * @param expr1
   */
  public RangeExpr(Expr expr0, Expr expr1)
  {
    super(new Expr[]{expr0, expr1});
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

//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
//   *      java.util.HashSet)
//   */
//  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
//      throws Exception
//  {
//    exprText.print(" (");
//    exprs[0].decompile(exprText, capturedVars);
//    exprText.print(") to (");
//    exprs[1].decompile(exprText, capturedVars);
//    exprText.print(") ");
//  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isConst()
   */
  @Override
  public boolean isConst()
  {
    // We only consider small ranges as a constant.
    // TODO: what is the right size?
    if (exprs[0] instanceof ConstExpr && exprs[1] instanceof ConstExpr)
    {
      long start = ((JLong) ((ConstExpr) exprs[0]).value.get()).value;
      long end = ((JLong) ((ConstExpr) exprs[1]).value.get()).value;
      if (end - start < 10)
      {
        return true;
      }
    }
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    JLong v1 = (JLong) exprs[0].eval(context).get();
    if (v1 == null)
    {
      return Iter.nil;
    }
    JLong v2 = (JLong) exprs[1].eval(context).get();
    if (v2 == null)
    {
      return Iter.nil;
    }
    final long start = v1.value;
    final long end = v2.value;

    return new Iter() {
      final JLong num  = new JLong(start - 1);
      final Item  item = new Item(num);

      public Item next()
      {
        if (num.value + 1 <= end)
        {
          num.value++;
          return item;
        }
        return null;
      }
    };
  }
}
