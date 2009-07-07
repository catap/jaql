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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;

/**
 * Run a list of pipes, perserving order as required.  Return the last pipe.
 * 
 * @author kbeyer
 *
 */
public class DoExpr extends Expr
{

  public DoExpr(Expr[] exprs)
  {
    super(exprs);
  }

  public DoExpr(Expr expr0)
  {
    super(expr0);
  }

  public DoExpr(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }

  public DoExpr(Expr expr0, Expr expr1, Expr expr2)
  {
    super(expr0, expr1, expr2);
  }

  public DoExpr(ArrayList<? extends Expr> exprs)
  {
    super(exprs);
  }
  
  /**
   * @return The Expr that returns a value to the caller.
   */
  public Expr returnExpr()
  {
    return exprs[exprs.length-1];
  }

  public Schema getSchema()
  {
    return exprs[exprs.length-1].getSchema();
  }
  
  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("(");
    String sep = "\n  ";
    for(Expr e: exprs)
    {
      exprText.print(sep);
      e.decompile(exprText, capturedVars);
      sep = ",\n  ";
    }
    exprText.println("\n)");
    for(Expr e: exprs)
    {
      if( e instanceof BindingExpr )
      {
        capturedVars.remove(((BindingExpr)e).var);
      }
    }
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    int n = exprs.length - 1;
    for(int i = 0 ; i < n ; i++)
    {
      // Item item = // TODO: expr.invoke()
      exprs[i].eval(context);
    }
    return exprs[n].eval(context);
  }

  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    int n = exprs.length - 1;
    for(int i = 0 ; i < n ; i++)
    {
      // Item item = // TODO: expr.invoke()
      exprs[i].eval(context);
    }
    return exprs[n].iter(context);
  }

}
