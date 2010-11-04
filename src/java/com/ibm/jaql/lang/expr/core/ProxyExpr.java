/*
 * Copyright (C) IBM Corp. 2009.
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

import java.util.HashSet;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.util.FastPrinter;


/**
 * A PseudoExpr that is used as a short-duration place-holder during rewrites.
 * 
 * For example:
 * 
 * Expr e = ...;
 * Expr proxy = new ProxyExpr();
 * e.replaceInParent(p);
 * Expr e2 = makeTreeSomeTree(e) // put e somewhere else
 * proxy.replaceInParent(e2);
 *
 */
public class ProxyExpr extends PseudoExpr
{

  /**
   * @param exprs
   */
  public ProxyExpr(Expr... exprs)
  {
    super(exprs);
  }

  /**
   * 
   */
  public ProxyExpr()
  {
    super(NO_EXPRS);
  }

  @Override
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("@proxy(");
    String sep = "";
    for( Expr e: exprs )
    {
      exprText.print(sep);
      e.decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print(")");
  }
  
}
