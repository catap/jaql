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
package com.ibm.jaql.lang.expr.array;

import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.CmpSingle;
import com.ibm.jaql.lang.expr.core.CmpSpec;
import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IndexExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.SortExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;

/**
 * 
 */
@JaqlFn(fnName = "reverse", minArgs = 1, maxArgs = 1)
public class ReverseFn extends MacroExpr
{
  /**
   * @param exprs
   */
  public ReverseFn(Expr[] exprs)
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
    // exprs[0] -> enumerate() -> sort by ($[0] desc) -> emit $[1] 
    Expr e = new EnumerateExpr(exprs[0]);
    Var v = env.makeVar("$");
    BindingExpr b = new BindingExpr(BindingExpr.Type.IN, v, null, e);
    CmpSingle by = new CmpSingle(new CmpSpec(new IndexExpr(new VarExpr(v), 0), CmpSpec.Order.DESC));
    DefineFunctionExpr cmp = new DefineFunctionExpr(new Var[] { v }, by);
    SortExpr sort = new SortExpr(b, cmp);
    b = new BindingExpr(BindingExpr.Type.IN, v, null, sort);
    e = new TransformExpr(b, new IndexExpr(new VarExpr(v), 1));
    return e;
  }
}
