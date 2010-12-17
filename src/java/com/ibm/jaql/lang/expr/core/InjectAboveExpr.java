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
 * A PseudoExpr that is used to easily inject an expression above
 * another expression.  For example:
 * 
 * inTreeExpr = some node that might be in an expression tree
 * newExpr = new FooExpr(new BarExpr(), inTreeExpr.injectAbove(), new BazExpr())
 * Now newExpr.parent() is what inTreeExpr.parent() used to be and that parent
 * points back to newExpr in the same slot that inTreeExpr was.  
 *
 */
public class InjectAboveExpr extends PseudoExpr
{

  /**
   * @param exprs
   */
  public InjectAboveExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   */
  public InjectAboveExpr()
  {
    super(NO_EXPRS);
  }

  /**
   * @param expr
   */
  public InjectAboveExpr(Expr expr)
  {
    super(expr);
  }

  @Override
  protected void decompileRaw(FastPrinter exprText, HashSet<Var> capturedVars, boolean emitLocation)
      throws Exception
  {
    exprText.print("@injectAbove(");
    if( exprs.length > 0 )
    {
      exprs[0].decompile(exprText, capturedVars,emitLocation);
    }
    exprText.print(")");
  }
  
}
