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
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JNumber;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;

/**
 * 
 */
public class IndexExpr extends Expr
{
  public IndexExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * array[index] (exprs[0])[ exprs[1] ]
   * 
   * @param array
   * @param index
   */
  public IndexExpr(Expr array, Expr index)
  {
    super(new Expr[]{array, index});
  }

  /**
   * @param expr
   * @param i
   */
  public IndexExpr(Expr expr, int i)
  {
    this(expr, new ConstExpr(JLong.sharedLongItem(i)));
  }

  /**
   * @return
   */
  public final Expr arrayExpr()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final Expr indexExpr()
  {
    return exprs[1];
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
    // TODO: use proper function?
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(")[");
    exprs[1].decompile(exprText, capturedVars);
    exprText.print("]");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    // TODO: support multiple indexes? $a[3 to 7], $a[ [3,4,5,6,7] ]
    // TODO: support array slices?  $a[3:7]
    Item item = exprs[1].eval(context);
    JValue w = item.get();
    if (w == null)
    {
      return Item.nil;
    }
    long i = ((JNumber) w).longValueExact();
    Expr arrayExpr = exprs[0];
    if (arrayExpr.isArray().always())
    {
      Iter iter = arrayExpr.iter(context);
      item = iter.nth(i);
    }
    else
    {
      item = arrayExpr.eval(context);
      // BUG: before was getNonNull, which is inconsistent with check.
      JArray array = (JArray) item.get();
      if (array == null)
      {
        return Item.nil;
      }
      item = array.nth(i);
    }
    return item;
  }
}
