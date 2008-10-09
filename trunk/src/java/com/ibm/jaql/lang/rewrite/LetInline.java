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
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.LetExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.hadoop.MRAggregate;
import com.ibm.jaql.lang.expr.hadoop.MapReduceFn;
import com.ibm.jaql.lang.expr.io.HBaseWriteExpr;
import com.ibm.jaql.lang.expr.io.HdfsWriteExpr;
import com.ibm.jaql.lang.expr.io.StReadExpr;
import com.ibm.jaql.lang.expr.io.WriteExpr;

/**
 * let ... $i = e1 ... $j = e2($i) ... return e3($i) => let ... $j = e2[$i ->
 * e1] ... return e3[$i -> e1] or e2[$i <- e1]
 * 
 * where e1 is not writing a temp
 */
public class LetInline extends Rewrite
{
  /**
   * @param phase
   */
  public LetInline(RewritePhase phase)
  {
    super(phase, LetExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    LetExpr letExpr = (LetExpr) expr;
    int numLetVars = letExpr.numBindings();
    Expr retExpr = letExpr.returnExpr();
    boolean replaced = false;

    for (int i = 0; i < numLetVars; i++)
    {
      BindingExpr b = (BindingExpr) letExpr.binding(i);
      Expr valExpr = b.eqExpr();

      // NOW: cannot inline if a side-affecting fn is anywhere in the ENTIRE subtree...
      if (valExpr instanceof WriteExpr
          || // FIXME: need to detect write exprs generically - actually need to detect side-effecting fns
          valExpr instanceof HdfsWriteExpr || valExpr instanceof HBaseWriteExpr
          || valExpr instanceof MapReduceFn || valExpr instanceof MRAggregate)
      {
        continue;
      }

      int numUses = countVarUse(letExpr, b.var);
      if (numUses == 0)
      {
        replaced = true;
      }
      else if (numUses == 1)
      {
        VarExpr use = findFirstVarUse(letExpr, b.var);
        use.replaceInParent(valExpr);
        replaced = true;
      }
      else
      // multiple uses of var
      {
        // TODO: else consider inlining cheap valExprs into multiple uses.
        if (valExpr instanceof ConstExpr || valExpr instanceof VarExpr
            || valExpr instanceof StReadExpr || valExpr.isConst())
        {
          replaceVarUses(b.var, letExpr, valExpr);
          replaced = true;
        }
      }

      if (replaced)
      {
        if (numLetVars == 1)
        {
          // Eliminate the LetExpr altogether.
          retExpr = letExpr.returnExpr(); // re-acquire the return expr, in case we modified it.
          letExpr.replaceInParent(retExpr);
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
