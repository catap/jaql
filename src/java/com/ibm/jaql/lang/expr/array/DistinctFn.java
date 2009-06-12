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
package com.ibm.jaql.lang.expr.array;

import java.util.ArrayList;

import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;

/**
 * 
 */
@JaqlFn(fnName = "distinct", minArgs = 1, maxArgs = 1)
public class DistinctFn extends MacroExpr
{
  /**
   * @param exprs
   */
  public DistinctFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.MacroExpr#expand(com.ibm.jaql.lang.core.Env)
   */
  @Override
  public Expr expand(Env env) throws Exception
  {
    // group $inVar in <expr0> by $byVar = $inVar into $intoVar collect [$byVar]
    Var inVar = env.makeVar("$distinctIn");
    Var byVar = env.makeVar("$distinctBy");
    Var intoVar = env.makeVar("$distinctInto");
    ArrayList<BindingExpr> bindings = new ArrayList<BindingExpr>(2);
    bindings.add(new BindingExpr(BindingExpr.Type.EQ, byVar, null, new VarExpr(
        inVar)));
    bindings
        .add(new BindingExpr(BindingExpr.Type.IN, inVar, intoVar, exprs[0]));
    Expr expr = new GroupByExpr(bindings, new ArrayExpr(new VarExpr(byVar)));
    return expr;
  }
}
