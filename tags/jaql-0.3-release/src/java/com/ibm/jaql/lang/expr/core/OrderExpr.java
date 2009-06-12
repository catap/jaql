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
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;

/**
 * An OrderExpr is not really an Expr at all. It is used to associate an
 * expression with an ordering option. It is used by Sort and GroupBy It should
 * never be evaluated or decompiled because the expressions that use bindings
 * know how to walk over these guys.
 */
public class OrderExpr extends Expr
{
  public static enum Order
  {
    ANY(), ASC(), DESC();
  }

  Order order;

  /**
   * @param exprs
   * @param order
   */
  public OrderExpr(Expr[] exprs, Order order)
  {
    super(exprs);
    this.order = order;
  }

  /**
   * @param expr
   * @param order
   */
  public OrderExpr(Expr expr, Order order)
  {
    super(new Expr[]{expr});
    this.order = order;
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
    throw new RuntimeException("OrderExpr should never be decompiled");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(Context context) throws Exception
  {
    throw new RuntimeException("OrderExpr should never be evaluated");
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
    return new OrderExpr(es[0], order);
  }

  /**
   * @return
   */
  public Expr orderExpr()
  {
    return exprs[0];
  }
}
