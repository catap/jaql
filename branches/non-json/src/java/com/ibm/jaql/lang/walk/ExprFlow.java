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
package com.ibm.jaql.lang.walk;

import java.util.ArrayList;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;

/**
 * 
 */
public class ExprFlow
{
  protected ArrayList<Expr> toVisit   = new ArrayList<Expr>();
  protected ExprWalker      useWalker = new PostOrderExprWalker();

  /**
   * @param b
   */
  protected void pushVarUses(BindingExpr b)
  {
    // TODO: make it easier to walk variable uses
    // for( VarExpr use = b.var.firstUse ; use != null ; use = use.nextUse )
    Expr expr;
    useWalker.reset(b.parent());
    while ((expr = useWalker.next()) != null)
    {
      if (expr instanceof VarExpr)
      {
        VarExpr ve = (VarExpr) expr;
        Var v = ve.var();
        if (v == b.var || v == b.var2)
        {
          toVisit.add(ve);
        }
      }
    }
  }

  protected Expr cur;

  /**
   * @param expr
   */
  public void reset(Expr expr)
  {
    toVisit.clear();
    if (expr != null)
    {
      toVisit.add(expr);
    }
  }

  /**
   * @return
   */
  public Expr next()
  {
    if (toVisit.size() == 0)
    {
      return null;
    }
    Expr result = toVisit.remove(toVisit.size() - 1);
    if (result.parent() != null)
    {
      toVisit.add(result.parent());
    }
    if (result instanceof BindingExpr)
    {
      pushVarUses((BindingExpr) result);
    }
    return result;
  }
}
