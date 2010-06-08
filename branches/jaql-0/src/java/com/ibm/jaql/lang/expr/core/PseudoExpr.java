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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;


public class PseudoExpr extends Expr
{

  public PseudoExpr(Expr[] exprs)
  {
    super(exprs);
  }

  public PseudoExpr(Expr expr0)
  {
    super(expr0);
  }

  public PseudoExpr(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }

  public PseudoExpr(Expr expr0, Expr expr1, Expr expr2)
  {
    super(expr0, expr1, expr2);
  }

  public PseudoExpr(ArrayList<? extends Expr> exprs)
  {
    super(exprs);
  }

  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("<<");
    exprText.print(getClass().getSimpleName());
    exprText.print(">>");
    exprText.print("(");
    String sep = "";
    for (Expr e : exprs)
    {
      exprText.print(sep);
      e.decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print(")");
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    throw new RuntimeException("PseudoExpr "+this.getClass().getName()+" cannot be evaluated!");
  }
}
