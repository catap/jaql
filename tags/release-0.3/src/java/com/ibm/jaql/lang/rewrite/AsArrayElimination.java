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

import com.ibm.jaql.lang.expr.array.AsArrayFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.CombineExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.IfExpr;

// TODO: This rewrite possibly go away with the change in FOR definition to preserve input.

/**
 * asArray(IterExpr) ==> IterExpr
 */
public class AsArrayElimination extends Rewrite
{
  /**
   * @param phase
   */
  public AsArrayElimination(RewritePhase phase)
  {
    super(phase, AsArrayFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    Expr input = expr.child(0);

    // asArray(null) -> []
    if (input.isNull().always())
    {
      expr.replaceInParent(new ArrayExpr());
      return true;
    }

    // asArray( non-nullable array expr ) -> expr
    if (input.isArray().always() && input.isNull().never())
    {
      expr.replaceInParent(input);
      return true;
    }

    //   for $i in asArray ...
    // | combine $a,$b in asArray ...
    // | group $i in asArray ...
    // =>
    // eliminate asArray
    if (expr.parent() instanceof BindingExpr)
    {
      Expr gp = expr.parent().parent();
      if (gp instanceof ForExpr || gp instanceof CombineExpr
          || gp instanceof GroupByExpr)
      {
        expr.replaceInParent(input);
        return true;
      }
    }

    // asArray( if t then e1 else e2 ) -> if t then asArray(e1) else asArray(e2)
    if (input instanceof IfExpr)
    {
      IfExpr ifExpr = (IfExpr) input;
      expr.replaceInParent(ifExpr); // remove asArray

      Expr e = ifExpr.trueExpr();
      if (e.isNull().always())
      {
        e.replaceInParent(new ArrayExpr());
      }
      else if (!(e.isArray().always() && e.isNull().never()))
      {
        ifExpr.setChild(1, new AsArrayFn(e));
      }

      e = ifExpr.falseExpr();
      if (e.isNull().always())
      {
        e.replaceInParent(new ArrayExpr());
      }
      else if (!(e.isArray().always() && e.isNull().never()))
      {
        ifExpr.setChild(2, new AsArrayFn(e));
      }

      return true;
    }

    return false;
  }
}
