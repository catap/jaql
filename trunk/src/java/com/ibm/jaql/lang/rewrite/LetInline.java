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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FunctionCallExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.hadoop.MRAggregate;
import com.ibm.jaql.lang.expr.hadoop.MapReduceFn;
import com.ibm.jaql.lang.expr.io.LocalWriteFn;
import com.ibm.jaql.lang.expr.io.ReadFn;
import com.ibm.jaql.lang.expr.io.WriteFn;
import com.ibm.jaql.lang.walk.ExprWalker;

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
    int n = doExpr.numChildren();
    boolean replaced = false;
    Expr e;

    assert n > 0; // There shouldn't be any empty do exprs
    
    for (int i = 0 ; i < n ; i++)
    {
      e = doExpr.child(i);
      if( ! (e instanceof BindingExpr) )
      {
        continue;
      }
      BindingExpr b = (BindingExpr) e;
      Expr valExpr = b.eqExpr();

      // FIXME: cannot inline if a side-affecting fn is anywhere in the ENTIRE subtree...
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
        // Do not inline a function variable that passes itself to itself (dynamic recursion)
        // conservative method: Inline only if every var usage goes directly into a function call. 
        if( valExpr instanceof DefineFunctionExpr   // TODO: we could try to make a Def into a Const
            || (valExpr instanceof ConstExpr && 
                ((ConstExpr)valExpr).value.get() instanceof JFunction) ) // TODO: we could inline Const fns because the fn is still shared via the const, but fn inline would need to check 
        {
          ExprWalker walker = engine.walker;
          walker.reset(expr);
          boolean allCalls = true;
          while ((e = walker.next()) != null)
          {
            if (e instanceof VarExpr)
            {
              VarExpr ve = (VarExpr) e;
              if (ve.var() == b.var)
              {
                // If not direct function call like $v(...), then don't inline 
                if( ve.getChildSlot() != 0 || !(ve.parent() instanceof FunctionCallExpr) )
                {
                  allCalls = false;
                  break;
                }
              }
            }
          }
          if( allCalls )
          {
            replaceVarUses(b.var, doExpr, valExpr);
            replaced = true;
          }
        }
        else if (   valExpr instanceof ConstExpr 
                 // || valExpr.isConst() // const expressions should be handled by compile-time eval to const
                 || valExpr instanceof VarExpr
                 || valExpr instanceof ReadFn ) 
        {
          replaceVarUses(b.var, doExpr, valExpr);
          replaced = true;
        }
        // TODO: else consider inlining cheap valExprs into multiple uses.
      }

      if (replaced)
      {
        if (n == 1) // do($v = e) ==> null
        {
          doExpr.replaceInParent(new ConstExpr(Item.NIL));
        }
        else if( i + 1 == n ) // do(e1,$v=e2) ==> (e1,null)
        {
          b.replaceInParent(new ConstExpr(Item.NIL));
        }
        else        // Eliminate the variable definition.
        {
          b.detach();
        }
        return true;
      }
    }

    if( n == 1 )  // do(e) == e
    {
      e = doExpr.child(0);
      assert !(e instanceof BindingExpr); // should have been handled above
      doExpr.replaceInParent(e);
      return true;
    }

    return false;
  }
}
