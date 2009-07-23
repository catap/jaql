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

import com.ibm.jaql.lang.core.JaqlFunction;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.FunctionCallExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.io.ReadFn;
import com.ibm.jaql.lang.walk.ExprWalker;

/**
 * ( $i = e1, $j = e2($i), e3($i,$j))
 * =>
 * ( $j = e2, e3($j) ) 
 * 
 * where 
 *   uses of $i replaced by e1
 *   e1 has no side-effects or non-determinism
 *   $i is used at most once and evaluated at most once or
 *      e1 is an AbstractReadExpr or
 *      e1 is a DefineFunctionExpr
 *      e1 is a ConstExpr
 *      e1 is non-array producing
 * 
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
      // We cannot inline subtrees that have side-effects of nondeterminism.
      // TODO: Another rewrite to split external effecting subtrees so a portion of the tree can be inlined.
      boolean noExternalEffects = 
        e.getProperty(ExprProperty.HAS_SIDE_EFFECTS, true).never() &&
        e.getProperty(ExprProperty.IS_NONDETERMINISTIC, true).never();
      
      if( e instanceof BindingExpr )
      {
        BindingExpr b = (BindingExpr) e;
        Expr valExpr = b.eqExpr();

        int numUses = countVarUse(doExpr, b.var);
        if (numUses == 0)  // (..., v=e, ...)   and v is never used
        {
          if( i < n - 1 )   // b is not the returned expression
          {
            if( noExternalEffects )
            {
              //     (..., v=e, e2, ...)   
              // ==> (..., e2, ...)        if v is never used and e has no external effect 
              b.detach();
            }
            else
            {
              //     (..., v=e, e2, ...)   
              // ==> (..., e, e2, ...)     if v is never used
              b.replaceInParent(valExpr);
            }
          }
          else // b is the returned expression
          {
            if( noExternalEffects )
            {
              //     (..., v=e)   
              // ==> (..., null)           if e has no external effect 
              b.replaceInParent(new ConstExpr(null));
            }
            else
            {
              //     (..., v=e)   
              // ==> (..., e, null)
              b.replaceInParent(valExpr);
              doExpr.addChild(new ConstExpr(null));
            }
          }
          return true;
        }
        
        if( valExpr.getProperty(ExprProperty.HAS_SIDE_EFFECTS, true).maybe() ||
            valExpr.getProperty(ExprProperty.IS_NONDETERMINISTIC, true).maybe() )
        {
          if( numUses == 1 )
          {
            VarExpr use = findFirstVarUse(doExpr, b.var);
            if( use.parent() == doExpr && use.getChildSlot() == b.getChildSlot() + 1 )
            {
              // ( ..., $v = <some effect>, $v, <...no use of $v...> ) ==> (..., <some effect>, <...no use of $v...>)   
              use.replaceInParent(valExpr);
              replaced = true;
            }
          }
          if( !replaced )
          {
            continue;
          }
        }

        if (!replaced && numUses == 1)
        {
          VarExpr use = findFirstVarUse(doExpr, b.var);
          if( evaluatedOnceTo(use, doExpr) ) // TODO: vars should be able to be marked as inlined to force inlining
          {
            use.replaceInParent(valExpr);
            replaced = true;
          }
        }

        if( !replaced )  // multiple uses of var or inlining might cause multiple evaluations
        {
          // Do not inline a function variable that passes itself to itself (dynamic recursion)
          // conservative method: Inline only if every var usage goes directly into a function call. 
          if( valExpr instanceof DefineFunctionExpr   // TODO: we could try to make a Def into a Const
              || (valExpr instanceof ConstExpr && 
                  ((ConstExpr)valExpr).value instanceof JaqlFunction) ) // TODO: we could inline Const fns because the fn is still shared via the const, but fn inline would need to check 
          {
            // If not direct function call like $v(...), then don't inline 
            if( allUsesAreCalls(b.var, expr) )
            {
              replaceVarUses(b.var, doExpr, valExpr);
              replaced = true;
            }
          }
          else if ( valExpr instanceof ConstExpr 
                 || valExpr instanceof VarExpr
                 || valExpr instanceof ReadFn ) 
          {
            replaceVarUses(b.var, doExpr, valExpr);
            replaced = true;
          }
          // TODO: else consider inlining cheap valExprs into multiple uses.
        }
      }
      else // not a BindingExpr
      {
        // (..., e, ...) ==> (..., ...)
        if( i < n - 1 && // not the returned expression
            e.getProperty(ExprProperty.HAS_SIDE_EFFECTS, true).never() &&
            e.getProperty(ExprProperty.IS_NONDETERMINISTIC, true).never() )
        {
          replaced = true;
        }
      }

      if (replaced)
      {
        if (n == 1) // do($v = e) ==> null
        {
          doExpr.replaceInParent(new ConstExpr(null));
        }
        else if( i + 1 == n ) // do(e1,$v=e2) ==> (e1,null)
        {
          e.replaceInParent(new ConstExpr(null));
        }
        else        // Eliminate the variable definition.
        {
          e.detach();
        }
        return true;
      }
    }

    return false;
  }

  /**
   * 
   * @param var
   * @param expr
   * @return True if all uses of var in expr are function calls: ie, v(...)
   */
  private boolean allUsesAreCalls(Var var, Expr expr)
  {
    ExprWalker walker = engine.walker;
    walker.reset(expr);
    Expr e;
    while ((e = walker.next()) != null)
    {
      if (e instanceof VarExpr)
      {
        VarExpr ve = (VarExpr) e;
        if (ve.var() == var)
        {
          if( ve.getChildSlot() != 0 || !(ve.parent() instanceof FunctionCallExpr) )
          {
            return false;
          }
        }
      }
    }
    return true;
  }
}
