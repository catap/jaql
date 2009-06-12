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
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;

/**
 * These are equivalent: 1. $x in $array 2. some $i in $array satisfies $i == $x
 * (when this syntax comes back to life) And nearly equivalent to: 3. (for $i in
 * $array where $i == $x return true)[0] 4. exists(for $i in $array where $i ==
 * $x return 'wow') 5. max(for $i in $array return $i == $x) Except when $array
 * is null: (1,2) return null, (3,4) return false when $array is empty: (1,2)
 * return false, (5) returns true We need a flavor of FOR that does not turn
 * null into empty.
 */
public class InExpr extends Expr
{
  /**
   * @param exprs
   */
  public InExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * expr0 in expr1
   * 
   * @param expr0
   * @param expr1
   */
  public InExpr(Expr expr0, Expr expr1)
  {
    super(new Expr[]{expr0, expr1});
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
    exprText.print(" (");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(") in (");
    exprs[1].decompile(exprText, capturedVars);
    exprText.print(") ");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    Item item1 = exprs[0].eval(context);
    if (item1.isNull())
    {
      return Item.nil;
    }
    Iter iter = exprs[1].iter(context);
    if (iter.isNull())
    {
      return Item.nil;
    }
    Item item2;
    Item result = JBool.falseItem;
    while ((item2 = iter.next()) != null)
    {
      if (item2.isNull())
      {
        result = Item.nil;
      }
      else if (item1.equals(item2))
      {
        result = JBool.trueItem;
        break;
      }
    }
    return result;
  }
}
