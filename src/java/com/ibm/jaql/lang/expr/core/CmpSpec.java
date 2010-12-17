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

import java.util.HashSet;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.DefaultJsonComparator;
import com.ibm.jaql.json.util.ReverseJsonComparator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JsonComparator;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.util.FastPrinter;

/**
 * An CmpExpr is not really an Expr at all. It is used to associate an
 * expression with an ordering option. It is used by Sort and GroupBy It should
 * never be evaluated or decompiled because the expressions that use bindings
 * know how to walk over these guys.
 */
public class CmpSpec extends Expr
{
  public static enum Order
  {
    ANY(), ASC(), DESC();
  }

  // usingFn
  Order order;
  // nulls first/last

  /**
   * @param exprs
   * @param order
   */
  public CmpSpec(Expr[] exprs, Order order)
  {
    super(exprs);
    this.order = order;
  }

  /**
   * @param expr
   * @param order
   */
  public CmpSpec(Expr expr, Order order)
  {
    super(new Expr[]{expr});
    this.order = order;
  }

  /**
   * @return
   */
  public Expr orderExpr()
  {
    return exprs[0];
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  protected void decompileRaw(FastPrinter exprText, HashSet<Var> capturedVars, boolean emitLocation)
      throws Exception
  {
    boolean parens = !( exprs[0] instanceof FieldExpr );
    if( parens ) exprText.print("(");
    exprs[0].decompile(exprText, capturedVars,emitLocation);
    if( parens ) exprText.print(")");
    if( order == Order.DESC )
    {
      exprText.print(" desc");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  protected JsonValue evalRaw(Context context) throws Exception
  {
    return exprs[0].eval(context);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  @Override
  public Expr clone(VarMap varMap)
  {
    Expr[] es = cloneChildren(varMap);
    return cloneOrigin(new CmpSpec(es[0], order));
  }

  /**
   * 
   * @param context
   * @return
   */
  public JsonComparator getComparator(Context context)
  {
    if( order == Order.DESC )
    {
      return new ReverseJsonComparator();
    }
    else
    {
      return new DefaultJsonComparator();
    }
  }
}
