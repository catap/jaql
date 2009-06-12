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
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;

/**
 * 
 */
public class AndExpr extends Expr
{
  /**
   * @param exprs
   */
  public AndExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr1
   * @param expr2
   */
  public AndExpr(Expr expr1, Expr expr2)
  {
    super(new Expr[]{expr1, expr2});
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
    exprText.print('(');
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(") and (");
    exprs[1].decompile(exprText, capturedVars);
    exprText.print(')');
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    Item item1 = exprs[0].eval(context);
    JBool b = (JBool) item1.get();
    if (b != null && b.getValue() == false)
    {
      return JBool.falseItem;
    }
    // item1 is true or null
    b = (JBool) exprs[1].eval(context).get();
    if (b == null)
    {
      return Item.nil;
    }
    if (b.getValue() == false)
    {
      return JBool.falseItem;
    }
    // b is true
    return item1;
  }
}
