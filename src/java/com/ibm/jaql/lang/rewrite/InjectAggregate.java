/*
 * Copyright (C) IBM Corp. 2009.
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
import com.ibm.jaql.lang.expr.agg.Aggregate;
import com.ibm.jaql.lang.expr.array.AsArrayFn;
import com.ibm.jaql.lang.expr.array.ToArrayFn;
import com.ibm.jaql.lang.expr.core.AggregateFullExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.ProxyExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.nil.EmptyOnNullFn;

// TODO: This rewrite possibly go away with the change in FOR definition to preserve input.

/**
 * e1 -> group ... expand as e2($) 
 * ==> e1 -> group ... expand ($ -> aggregate each $i e3($i))
 * where every use of $ in e2 is:
 *      aggfn( $ )
 *      aggfn( for( $j in $ ) e4 )
 *      aggfn( $ -> transform e4 )
 *      // TODO: should be aggfn( anyIteratingUse($) )
 * becomes in e3:
 *      aggfn( $i )
 *      aggfn( for( $j in $i ) e4 )
 *      aggfn( $i -> transform e4 )
 */
public class InjectAggregate extends Rewrite
{
  /**
   * @param phase
   */
  public InjectAggregate(RewritePhase phase)
  {
    super(phase, GroupByExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    GroupByExpr g = (GroupByExpr)expr;
    if( g.numInputs() != 1 )
    {
      return false;
    }
    Var groupVar = g.getAsVar(0);
    Expr collect = g.collectExpr();

    // Check if every use of the group variable is used appropriately in an aggregate function.
    engine.exprList.clear();
    collect.getVarUses(groupVar, engine.exprList);
    if( engine.exprList.size() == 0 )
    {
      // Don't inject aggregate if there are no aggregate functions.
      return false;
    }
    for( Expr e: engine.exprList )
    {
      assert e instanceof VarExpr;
      int slot = e.getChildSlot();
      Expr p = e.parent();
      if( p instanceof ToArrayFn ||
          p instanceof AsArrayFn ||
          p instanceof EmptyOnNullFn )
      {
        slot = p.getChildSlot();
        p = p.parent();
      }
      while( p != g && !(p instanceof Aggregate) )
      {
        if( p instanceof BindingExpr )
        {
          slot = p.getChildSlot();
          p = p.parent();
        }
        if( ! p.isMappable(slot) )
        {
          return false;
        }
        slot = p.getChildSlot();
        p = p.parent();
      }
      if( p == g )
      {
        return false;
      }
    }
    // We can transform!
    
    // Replace all uses of the group variable with the aggregate variable.
    g.getSchema();
    Var aggVar = engine.env.makeVar("$", groupVar.getSchema());
    for( Expr e: engine.exprList )
    {
      ((VarExpr)e).setVar(aggVar);
    }
    // Inject the aggregate expr
    Expr proxy = new ProxyExpr();
    collect.replaceInParent(proxy);
    BindingExpr b = new BindingExpr(BindingExpr.Type.EQ, aggVar, null, new VarExpr(groupVar));
    Expr agg = AggregateFullExpr.make(engine.env, b, collect, true);
    proxy.replaceInParent(agg);

    return true;
  }
}
