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
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.LetExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.pragma.InlinePragma;

/**
 * 
 */
public class DoInlinePragma extends Rewrite
{
  /**
   * @param phase
   */
  public DoInlinePragma(RewritePhase phase)
  {
    super(phase, InlinePragma.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    assert expr instanceof InlinePragma;
    Expr c = expr.child(0);
    if (c instanceof VarExpr)
    {
      VarExpr varExpr = (VarExpr) c;
      Var var = varExpr.var();
      Expr def = findVarDef(varExpr);
      if (def == null)
      {
        // must be a function parameter // TODO: findVarDef SHOULD find it, and params should use Bindings
        return false;
      }
      else if (def instanceof BindingExpr)
      {
        BindingExpr b = (BindingExpr) def;
        if (b.parent() instanceof LetExpr && var == b.var)
        {
          expr.replaceInParent(cloneExpr(b.eqExpr()));
          return true;
        }
      }
      else
      {
        assert var.isGlobal();
        Expr replaceBy;
        if (var.value != null)
        {
          // If the global is already computed, inline its value
          replaceBy = new ConstExpr(var.value);
        }
        else
        {
          // If the global is not already computed, inline its expr
          replaceBy = cloneExpr(def);
        }
        expr.replaceInParent(replaceBy);
        return true;
      }
    }

    // If this inline request is not over a VarExpr for a let variable or global variable, just remove it.
    expr.replaceInParent(c);
    return true;
  }
}
