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

import java.util.ArrayList;

import com.ibm.jaql.lang.core.Env;

/**
 * 
 */
public class MultiForExpr extends MacroExpr
{
  private static Expr[] makeArgs(ArrayList<BindingExpr> bindings, Expr ifExpr,
      Expr doExpr)
  {
    Expr[] exprs = new Expr[bindings.size() + 2];
    exprs[0] = ifExpr; // might be null
    exprs[1] = doExpr;
    for (int i = 0; i < bindings.size(); i++)
    {
      BindingExpr b = bindings.get(i);
      assert b.type == BindingExpr.Type.IN || b.type == BindingExpr.Type.INREC;
      exprs[2 + i] = b;
    }
    return exprs;
  }

  /**
   * @param bindings
   * @param ifExpr
   * @param doExpr
   */
  public MultiForExpr(ArrayList<BindingExpr> bindings, Expr ifExpr, Expr doExpr)
  {
    // Right now references to binding exprs are stored twice; once in the binding
    // and once in the Expr superclass.  We will either need to keep the consistent or
    // find a way to get rid of one of them.
    super(makeArgs(bindings, ifExpr, doExpr));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  //  public void decompile(PrintStream exprText, HashSet<Var> capturedVars) throws Exception
  //  {
  //    final Expr ifExpr = exprs[0];
  //    final Expr doExpr = exprs[1];
  //    
  //    exprText.print("\nfor ");
  //    String sep = "";
  //    for(int i = 2 ; i < exprs.length ; i++)
  //    {
  //      BindingExpr b = (BindingExpr)exprs[i];
  //      exprText.print(sep);
  //      exprText.print(b.var.name);
  //      switch( b.type )
  //      {
  //        case IN:
  //          if( b.var2 != null )
  //          {
  //            exprText.print(" at ");
  //            exprText.print(b.var2.name);
  //          }
  //          exprText.print(" in ");
  //          break;
  //
  //        case INREC:
  //          exprText.print(":");
  //          exprText.print(b.var2.name);
  //          exprText.print(" in ");
  //          break;
  //          
  //        default:
  //          throw new RuntimeException("bad binding type");
  //      }
  //      exprText.print("(");
  //      b.inExpr().decompile(exprText, capturedVars);
  //      exprText.print(")");
  //      sep = ",\n";
  //    }
  //    if( !(ifExpr instanceof TrueExpr) )
  //    {
  //      exprText.print("where (");
  //      ifExpr.decompile(exprText, capturedVars);
  //      exprText.println(")");      
  //    }
  //    exprText.print("return (");
  //    doExpr.decompile(exprText, capturedVars);
  //    exprText.println(")");
  //
  //    for(int i = 2 ; i < exprs.length ; i++)
  //    {
  //      BindingExpr b = (BindingExpr)exprs[i];
  //      capturedVars.remove(b.var);
  //      if( b.var2 != null )
  //      {
  //        capturedVars.remove(b.var2);
  //      }
  //    }
  //  }

  /**
   * @param env
   * @return
   */
  public Expr expand(Env env) // throws Exception
  {
    Expr ifExpr = exprs[0];
    Expr expr = exprs[1]; // doExpr

    // expr = new ArrayExpr(expr);

    if (ifExpr != null)
    {
      expr = new IfExpr(ifExpr, expr);
    }

    //boolean unnest = false;
    for (int i = exprs.length - 1; i >= 2; i--)
    {
      BindingExpr b = (BindingExpr) exprs[i];
      switch (b.type)
      {
        case IN :
          // expr = new ForExpr(unnest, b, ifExpr, expr );
          expr = new ForExpr(b.var, b.inExpr(), expr);
          break;

//        case INREC : // TODO: this is not used anymore, use: for $f in fields($rec)
//          // expr = new WithInRecExpr(unnest, b, ifExpr, expr );
//          Var pairVar = env.makeVar("$kvpair");
//          expr = new LetExpr(b.var, new IndexExpr(new VarExpr(pairVar), 0),
//              b.var2, new IndexExpr(new VarExpr(pairVar), 1), expr);
//          expr = new ForExpr(pairVar, new FieldsFn(b.inExpr()), expr);
//          break;

        default :
          throw new RuntimeException("bad binding type");
      }
      // ifExpr = null;
      // unnest = true;
    }
    return expr;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
//  public JsonIterator iter(final Context context) throws Exception
//  {
//    throw new RuntimeException("this should have been expanded!"); // expand it...
//  }
}
