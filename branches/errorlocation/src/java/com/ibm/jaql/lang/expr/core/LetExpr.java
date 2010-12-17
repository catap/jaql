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

// TODO: kill this code

//package com.ibm.jaql.lang.expr.core;
//
//import java.io.PrintStream;
//import java.util.ArrayList;
//import java.util.HashSet;
//
//import com.ibm.jaql.json.type.Item;
//import com.ibm.jaql.json.util.Iter;
//import com.ibm.jaql.lang.core.Context;
//import com.ibm.jaql.lang.core.Var;
//import com.ibm.jaql.util.Bool3;
//
///**
// * 
// */
//public final class LetExpr extends Expr
//{
//  /**
//   * @param bindings
//   * @param doExpr
//   * @return
//   */
//  private static Expr[] makeArgs(ArrayList<BindingExpr> bindings, Expr doExpr)
//  {
//    int n = bindings.size();
//    Expr[] exprs = new Expr[n + 1];
//    for (int i = 0; i < n; i++)
//    {
//      BindingExpr b = bindings.get(i);
//      assert b.type == BindingExpr.Type.EQ;
//      exprs[i] = b;
//    }
//    exprs[n] = doExpr;
//    return exprs;
//  }
//
//  /**
//   * @param exprs
//   */
//  public LetExpr(Expr[] exprs)
//  {
//    super(exprs);
//  }
//
//  /**
//   * exprs = [ (inBinding)+ doExpr ]
//   * 
//   * @param bindings
//   * @param doExpr
//   */
//  public LetExpr(ArrayList<BindingExpr> bindings, Expr doExpr)
//  {
//    super(makeArgs(bindings, doExpr));
//  }
//
//  /**
//   * @param binding
//   * @param doExpr
//   */
//  public LetExpr(BindingExpr binding, Expr doExpr)
//  {
//    super(new Expr[]{binding, doExpr});
//  }
//
//  /**
//   * @param binding1
//   * @param binding2
//   * @param doExpr
//   */
//  public LetExpr(BindingExpr binding1, BindingExpr binding2, Expr doExpr)
//  {
//    super(new Expr[]{binding1, binding2, doExpr});
//  }
//
//  /**
//   * @param letVar
//   * @param eqExpr
//   * @param doExpr
//   */
//  public LetExpr(Var letVar, Expr eqExpr, Expr doExpr)
//  {
//    this(new BindingExpr(BindingExpr.Type.EQ, letVar, null, eqExpr), doExpr);
//  }
//
//  /**
//   * @param var1
//   * @param eqExpr1
//   * @param var2
//   * @param eqExpr2
//   * @param doExpr
//   */
//  public LetExpr(Var var1, Expr eqExpr1, Var var2, Expr eqExpr2, Expr doExpr)
//  {
//    this(new BindingExpr(BindingExpr.Type.EQ, var1, null, eqExpr1),
//        new BindingExpr(BindingExpr.Type.EQ, var2, null, eqExpr2), doExpr);
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
//   */
//  @Override
//  public Bool3 isNull()
//  {
//    return returnExpr().isNull();
//  }
//
//  /**
//   * @return
//   */
//  public final int numBindings()
//  {
//    return exprs.length - 1;
//  }
//
//  /**
//   * @param i
//   * @return
//   */
//  public final BindingExpr binding(int i)
//  {
//    return (BindingExpr) exprs[i];
//  }
//
//  /**
//   * @return
//   */
//  public final Expr returnExpr()
//  {
//    return exprs[exprs.length - 1];
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
//   *      java.util.HashSet)
//   */
//  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
//      throws Exception
//  {
//    //    exprText.print("\nlet( ");
//    //    int n = exprs.length - 1;
//    //    String sep = "";
//    //    for(int i = 0 ; i < n ; i++)
//    //    {
//    //      BindingExpr b = binding(i);
//    //      exprText.print(sep);      
//    //      exprText.print(b.var.name);
//    //      exprText.print(" = ");
//    //      b.eqExpr().decompile(exprText, capturedVars,emitLocation);      
//    //      sep = ",\n     ";
//    //    }
//    //    exprText.println(")");
//    //    returnExpr().decompile(exprText, capturedVars,emitLocation);
//
//    exprText.println("(");
//    int n = exprs.length - 1;
//    for (int i = 0; i < n; i++)
//    {
//      BindingExpr b = binding(i);
//      exprText.print("    ");
//      exprText.print(b.var.name);
//      exprText.print(" = ");
//      b.eqExpr().decompile(exprText, capturedVars,emitLocation);
//      exprText.println(",");
//    }
//    exprText.println();
//    returnExpr().decompile(exprText, capturedVars,emitLocation);
//    exprText.println(")");
//
//    for (int i = 0; i < n; i++)
//    {
//      BindingExpr b = binding(i);
//      capturedVars.remove(b.var);
//    }
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#isArray()
//   */
//  @Override
//  public Bool3 isArray()
//  {
//    return returnExpr().isArray();
//  }
//
//  /**
//   * @param context
//   * @throws Exception
//   */
//  private void evalLets(Context context) throws Exception
//  {
//    int n = exprs.length - 1;
//    Item item;
//    for (int i = 0; i < n; i++)
//    {
//      BindingExpr b = binding(i);
//      item = b.eqExpr().eval(context);
//      context.setVar(b.var, item);
//    }
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
//   */
//  protected Item evalRaw(Context context) throws Exception
//  {
//    evalLets(context);
//    Item item = returnExpr().eval(context);
//    return item;
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#iter(com.ibm.jaql.lang.core.Context)
//   */
//  @Override
//  public Iter iter(Context context) throws Exception
//  {
//    evalLets(context);
//    return returnExpr().iter(context);
//  }
//
//}
