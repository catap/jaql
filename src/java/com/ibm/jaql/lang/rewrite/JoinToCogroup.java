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

import java.util.ArrayList;

import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.IfExpr;
import com.ibm.jaql.lang.expr.core.JoinExpr;
import com.ibm.jaql.lang.expr.core.NotExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.nil.IsnullFn;
import com.ibm.jaql.lang.expr.nil.NullElementOnEmptyFn;

/**
 * 
 */
public class JoinToCogroup extends Rewrite
{
  /**
   * @param phase
   */
  public JoinToCogroup(RewritePhase phase)
  {
    super(phase, JoinExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    // join preserve? $i in ei1 on ei2($i),
    //      preserve? $j in ej1 on ej2($j),
    //       ...
    // return er($i,$j)
    //
    // group $i in ei1 by $tg = ei2($i) into $ti,
    //       $j in ej1 by $tg = ej2($j) into $tj,
    //       ...
    // collect
    //    for $ui in $ti | nullElementOnEmpty($ti)
    //        $uj in $tj | nullElementOnEmpty($tj),
    //        ...
    //    where not(isnull($tg))
    //    return er($ui, $uj)
    JoinExpr join = (JoinExpr) expr;

    ArrayList<BindingExpr> bindings = new ArrayList<BindingExpr>();

    int n = join.numBindings();
    assert n > 1;

    Env env = engine.env;
    Var byVar = env.makeVar("$join_on");
    BindingExpr byBinding = new BindingExpr(BindingExpr.Type.EQ, byVar, null,
        new Expr[n]);
    bindings.add(byBinding);
    Expr joinCollect = join.collectExpr();

    // Expr groupCollect = new ArrayExpr(joinCollect);
    Expr groupCollect = joinCollect;
    for (int i = 0; i < n; i++)
    {
      BindingExpr joinBinding = join.binding(i);
      Var intoVar = env.makeVar("$join_into_" + i);
      Var iterVar = env.makeVar("$join_iter_" + i);
      Expr inExpr = joinBinding.inExpr();
      BindingExpr groupBinding = new BindingExpr(BindingExpr.Type.IN,
          joinBinding.var, intoVar, inExpr);
      bindings.add(groupBinding);
      byBinding.setChild(i, joinBinding.onExpr());
      joinCollect.replaceVar(joinBinding.var, iterVar);
      inExpr = new VarExpr(intoVar);
      if (joinBinding.optional)
      {
        inExpr = new NullElementOnEmptyFn(inExpr);
      }

      // groupRet = new ForExpr(i != 0, iterVar, null, inExpr, null, groupRet);
      groupCollect = new ForExpr(iterVar, inExpr, groupCollect);
    }

    // drop the null keys
    groupCollect = new IfExpr(new NotExpr(new IsnullFn(new VarExpr(byVar))),
        groupCollect);

    Expr groupBy = new GroupByExpr(bindings, groupCollect);
    // groupBy = new UnnestExpr( groupBy );

    expr.replaceInParent(groupBy);

    return true;
  }
}
