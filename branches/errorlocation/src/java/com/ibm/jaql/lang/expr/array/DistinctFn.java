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

import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.GroupByExpr;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription List distinct values from an array, remove duplicates.
 * 
 * Usage:
 * [any] distinct( [any] )
 * 
 * @jaqlExample distinct( [1, 1d, 1m, 1.5d, 1.5m, 1.50d, 1.50m ] ) -> sort by [$];
 * [ 1,1.5 ]
 */
public class DistinctFn extends MacroExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("distinct", DistinctFn.class);
    }
  }
  
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
  public Expr expandRaw(Env env) throws Exception
  {
    // group $inVar in <expr0> by $byVar = $inVar into $intoVar collect [$byVar]
    Var v = env.makeVar("$");
    Var by = env.makeVar("$key");
    Var as = env.makeVar("$as");
    Expr r = new GroupByExpr(
        new BindingExpr(BindingExpr.Type.IN, v, null, exprs[0]),
        new BindingExpr(BindingExpr.Type.EQ, by, null, new VarExpr(v)),
        as,
        null, // no comparator
        null, // TODO: how does distinct specify options? 
        new ArrayExpr(new VarExpr(by)));
    return r;
  }
}
