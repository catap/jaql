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
package com.ibm.jaql.lang.rewrite;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FunctionCallExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.hadoop.MRAggregate;
import com.ibm.jaql.lang.expr.hadoop.MapReduceFn;
import com.ibm.jaql.lang.walk.ExprFlow;
import com.ibm.jaql.lang.walk.ExprWalker;

/**
 * 
 */
public abstract class Rewrite
{
  protected RewriteEngine engine;

  /**
   * @param phase
   * @param fireOn
   */
  public Rewrite(RewritePhase phase, Class<? extends Expr> fireOn)
  {
    this.engine = phase.engine;
    phase.fireOn(this, fireOn);
  }

  /**
   * @param phase
   * @param fireOn
   */
  public Rewrite(RewritePhase phase, Class<? extends Expr>[] fireOn)
  {
    this.engine = phase.engine;
    phase.fireOn(this, fireOn);
  }

  /**
   * @param expr
   * @return
   * @throws Exception
   */
  public abstract boolean rewrite(Expr expr) throws Exception;

  // Rewrite utilities.
  // Is there a better place for these?

  /**
   * @param expr
   * @return
   */
  protected Expr cloneExpr(Expr expr)
  {
    engine.varMap.clear();
    return expr.clone(engine.varMap);
  }

  /**
   * @param varExpr
   * @return
   */
  protected Expr findVarDef(VarExpr varExpr)
  {
    Var var = varExpr.var();
    if (var.isGlobal())
    {
      return var.expr;
    }
    for (Expr e = varExpr.parent(); e != null; e = e.parent())
    {
      if (e instanceof DefineFunctionExpr)
      {
        for (Var p : ((DefineFunctionExpr) e).params())
        {
          if (p == var)
          {
            return e;
          }
        }
      }
      else
      {
        for (Expr c : e.children())
        {
          if (c instanceof BindingExpr)
          {
            BindingExpr b = (BindingExpr) c;
            if (b.var == var || b.var2 == var)
            {
              return b;
            }
          }
        }
      }
    }
    return null;
  }

  // FIXME: finding a variable by name might not be done correctly in all cases...
  /**
   * @param atExpr
   * @param varName
   * @return
   */
  protected Expr findVarDef(Expr atExpr, String varName)
  {
    Expr prev = atExpr;
    for (Expr e = atExpr.parent(); e != null; e = e.parent())
    {
      if (e instanceof DefineFunctionExpr)
      {
        for (Var p : ((DefineFunctionExpr) e).params())
        {
          if (p.name.equals(varName))
          {
            return e;
          }
        }
      }
      else
      {
        int i = prev.getChildSlot();
        // look for bindings as direct children after us
        for (i = i - 1; i >= 0; i--)
        {
          Expr c = e.child(i);
          if (c instanceof BindingExpr)
          {
            BindingExpr b = (BindingExpr) c;
            if (varName.equals(b.var.name)
                || (b.var2 != null && varName.equals(b.var2.name)))
            {
              return b;
            }
          }
        }
      }
      prev = e;
    }
    return null;
  }

  // FIXME: There is a potential bug here when exprTree == VarExpr that gets replaced!
  /**
   * @param var
   * @param exprTree
   * @param replaceBy
   * @return
   */
  protected int replaceVarUses(Var var, Expr exprTree, Expr replaceBy)
  {
    ExprWalker walker = engine.walker;
    walker.reset(exprTree);
    Expr expr = walker.next();
    int n = 0;
    while (expr != null)
    {
      if (expr instanceof VarExpr)
      {
        VarExpr ve = (VarExpr) expr;
        expr = walker.next();
        if (ve.var() == var)
        {
          ve.replaceInParent(cloneExpr(replaceBy));
          n++;
        }
      }
      else
      {
        expr = walker.next();
      }
    }
    return n;
  }

  // TODO: this needs to exclude the case where one map/reduce is input to another. 
  /**
   * Determine if this Expr could be part of a map, combine, or reduce function
   * to a map/reduce job. Right now it is extremely conservative: it looks to
   * see if this expr is in a function that could be input to one of the
   * map/reduce functions.
   * 
   * @param expr
   * @return
   */
  public boolean maybeInMapReduce(Expr expr)
  {
    ExprFlow flow = engine.flow;
    flow.reset(expr);

    while ((expr = flow.next()) != null)
    {
      if (expr instanceof DefineFunctionExpr)
      {
        break;
      }
    }

    while ((expr = flow.next()) != null)
    {
      if (expr instanceof MapReduceFn || expr instanceof MRAggregate)
      {
        return true;
      }
    }
    return false;
  }

  /**
   * @param expr
   * @return
   */
  public boolean mightContainMapReduce(Expr expr)
  {
    ExprWalker walker = engine.walker;
    walker.reset(expr);

    while ((expr = walker.next()) != null)
    {
      if (expr instanceof MapReduceFn || expr instanceof MRAggregate)
      {
        return true;
      }
      if (expr instanceof FunctionCallExpr)
      {
        if (!(expr instanceof DefineFunctionExpr))
        {
          // FIXME: look deeper into this case.
          return true;
        }
        // For FunctionExpr, continue into the body
      }
    }
    return false;
  }

  /**
   * @param expr
   * @param var
   * @return
   */
  int countVarUse(Expr expr, Var var)
  {
    int n = 0;
    ExprWalker walker = engine.walker;
    walker.reset(expr);
    while ((expr = walker.next()) != null)
    {
      if (expr instanceof VarExpr)
      {
        VarExpr ve = (VarExpr) expr;
        if (ve.var() == var)
        {
          n++;
        }
      }
    }
    return n;
  }

  /**
   * @param expr
   * @param var
   * @return
   */
  VarExpr findFirstVarUse(Expr expr, Var var)
  {
    ExprWalker walker = engine.walker;
    walker.reset(expr);

    while ((expr = walker.next()) != null)
    {
      if (expr instanceof VarExpr)
      {
        VarExpr ve = (VarExpr) expr;
        if (ve.var() == var)
        {
          return (VarExpr) expr;
        }
      }
    }
    return null;
  }
}
