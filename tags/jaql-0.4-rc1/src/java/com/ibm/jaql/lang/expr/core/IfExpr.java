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
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
public final class IfExpr extends Expr
{
  /**
   * @param testExpr
   * @param trueExpr
   * @param falseExpr
   * @return
   */
  private static Expr[] makeArgs(Expr testExpr, Expr trueExpr, Expr falseExpr)
  {
    if (falseExpr == null)
    {
      falseExpr = new ConstExpr(Item.nil);
    }
    return new Expr[]{testExpr, trueExpr, falseExpr};
  }

  /**
   * @param exprs
   */
  public IfExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * if exprs[0] then exprs[1] else exprs[2]?
   * 
   * @param testExpr
   * @param trueExpr
   * @param falseExpr
   */
  public IfExpr(Expr testExpr, Expr trueExpr, Expr falseExpr)
  {
    super(makeArgs(testExpr, trueExpr, falseExpr));
  }

  /**
   * @param testExpr
   * @param trueExpr
   */
  public IfExpr(Expr testExpr, Expr trueExpr)
  {
    super(makeArgs(testExpr, trueExpr, null));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isArray()
   */
  @Override
  public Bool3 isArray()
  {
    return exprs[1].isArray().both(exprs[2].isArray());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
   */
  @Override
  public Bool3 isNull()
  {
    return exprs[1].isNull().both(exprs[2].isNull());
  }

  /**
   * @return
   */
  public final Expr testExpr()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final Expr trueExpr()
  {
    return exprs[1];
  }

  /**
   * @return
   */
  public final Expr falseExpr()
  {
    return exprs[2];
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
    exprText.print("\nif( ");
    testExpr().decompile(exprText, capturedVars);
    exprText.print(" )\n( ");
    trueExpr().decompile(exprText, capturedVars);
    if (falseExpr().isNull().maybeNot())
    {
      exprText.print(" )\nelse ( ");
      falseExpr().decompile(exprText, capturedVars);
    }
    exprText.println(" )");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    boolean b = JaqlUtil.ebv(exprs[0].eval(context));
    Item item;
    if (b)
    {
      item = exprs[1].eval(context);
    }
    else
    {
      item = exprs[2].eval(context);
    }
    return item;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    boolean b = JaqlUtil.ebv(exprs[0].eval(context));
    Iter iter;
    if (b)
    {
      iter = exprs[1].iter(context);
    }
    else
    {
      iter = exprs[2].iter(context);
    }
    return iter;
  }
}
