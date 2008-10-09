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
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.core.Var;

// TODO: optimize the case when the fn is known to have a IterExpr body
/**
 * 
 */
public class FunctionCallExpr extends Expr
{
  Item[] args;

  /**
   * exprs[0](exprs[1:*])
   * 
   * @param exprs
   */
  public FunctionCallExpr(Expr[] exprs)
  {
    super(exprs);
    args = new Item[exprs.length - 1];
  }

  /**
   * @param fn
   * @param args
   * @return
   */
  private static Expr[] makeExprs(Expr fn, ArrayList<Expr> args)
  {
    Expr[] exprs = new Expr[args.size() + 1];
    exprs[0] = fn;
    for (int i = 1; i < exprs.length; i++)
    {
      exprs[i] = args.get(i - 1);
    }
    return exprs;
  }

  /**
   * @param fn
   * @param args
   */
  public FunctionCallExpr(Expr fn, ArrayList<Expr> args)
  {
    this(makeExprs(fn, args));
  }

  /**
   * @return
   */
  public final Expr fnExpr()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final int numArgs()
  {
    return exprs.length - 1;
  }

  /**
   * @param i
   * @return
   */
  public final Expr arg(int i)
  {
    return exprs[i + 1];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(")");
    exprText.print("(");
    char sep = ' ';
    for (int i = 1; i < exprs.length; i++)
    {
      exprText.print(sep);
      exprText.print("(");
      exprs[i].decompile(exprText, capturedVars);
      exprText.print(")");
      sep = ',';
    }
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Item eval(Context context) throws Exception
  {
    JFunction fn = (JFunction) exprs[0].eval(context).get();
    if (fn == null)
    {
      return Item.nil;
    }
    for (int i = 1; i < exprs.length; i++)
    {
      args[i - 1] = exprs[i].eval(context);
    }
    return fn.eval(context, args);
  }

}
