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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.top.AssignExpr;
import com.ibm.jaql.util.Bool3;

/**
 * Run a list of pipes, perserving order as required.  Return the last pipe.
 * 
 * @author kbeyer
 *
 */
public class DoExpr extends Expr
{
  /**
   * If the last expression is a BindingExpr, add a ConstExpr(null) for the return expr.
   * A DoExpr should never end with a BindingExpr, even after rewrites.
   */
  protected static Expr[] canonicalForm(Expr[] exprs)
  {
    int n = exprs.length;
    if( exprs[n-1] instanceof BindingExpr )
    {
      exprs = Arrays.copyOf(exprs, n + 1);
      exprs[n] = new ConstExpr(null);
    }
    return exprs;
  }
  
  public DoExpr(Expr ... exprs)
  {
    super(canonicalForm(exprs));
  }

  public DoExpr(ArrayList<? extends Expr> exprs)
  {
    this(exprs.toArray(new Expr[exprs.size()]));
  }
  
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    return result;
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
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
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
      if( e instanceof AssignExpr )
      {
        capturedVars.remove(((AssignExpr)e).var);
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
