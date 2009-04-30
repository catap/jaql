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

import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.hadoop.MRAggregate;
import com.ibm.jaql.lang.expr.hadoop.MapReduceFn;
import com.ibm.jaql.lang.expr.io.LocalWriteFn;
import com.ibm.jaql.lang.expr.io.ReadFn;
import com.ibm.jaql.lang.expr.io.WriteFn;

/**
 * let ... $i = e1 ... $j = e2($i) ... return e3($i) => let ... $j = e2[$i ->
 * e1] ... return e3[$i -> e1] or e2[$i <- e1]
 * 
 * where e1 is not writing a temp
 */
public class LetInline extends Rewrite // TODO: rename to Var inline
{
  /**
   * @param phase
   */
  public LetInline(RewritePhase phase)
  {
    super(phase, DoExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    DoExpr doExpr = (DoExpr) expr;
    int n = doExpr.numChildren() - 1;
    boolean replaced = false;
    Expr e;

    assert n >= 0;
    if( n == 0 )  // do(e) == e
    {
      e = doExpr.child(0);
      doExpr.replaceInParent(e);
      return true;
    }
    
    for (int i = 0 ; i < n ; i++)
    {
      e = doExpr.child(i);
      if( ! (e instanceof BindingExpr) )
      {
        continue;
      }
      BindingExpr b = (BindingExpr) e;
      Expr valExpr = b.eqExpr();

      // NOW: cannot inline if a side-affecting fn is anywhere in the ENTIRE subtree...
      if (valExpr instanceof WriteFn // FIXME: need to detect write exprs generically - actually need to detect side-effecting fns
          || valExpr instanceof LocalWriteFn
          || valExpr instanceof MapReduceFn || valExpr instanceof MRAggregate)
      {
        continue;
      }

      int numUses = countVarUse(doExpr, b.var);
      if (numUses == 0)
      {
        replaced = true;
      }
      else if (numUses == 1)
      {
        VarExpr use = findFirstVarUse(doExpr, b.var);
        use.replaceInParent(valExpr);
        replaced = true;
      }
      else // multiple uses of var
      {
        // TODO: else consider inlining cheap valExprs into multiple uses.
        if (valExpr instanceof ConstExpr || valExpr instanceof VarExpr
            // || valExpr.isConst() // const expressions should be handled by compile-time eval to const
            || valExpr instanceof DefineFunctionExpr
            || valExpr instanceof ReadFn ) 
        {
          replaceVarUses(b.var, doExpr, valExpr);
          replaced = true;
        }
      }

      if (replaced)
      {
        if (n == 1)
        {
          // Eliminate the DoExpr altogether.
          e = doExpr.child(1);
          doExpr.replaceInParent(e);
        }
        else
        {
          // Eliminate the variable definition.
          b.detach();
        }
        return true;
      }
    }

    return false;
  }
}
