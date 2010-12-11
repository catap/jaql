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
package com.ibm.jaql.lang.expr.nil;

import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.IfExpr;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.NotExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription remove nulls from a given array
 * Usage:
 * [T] denull([T]);
 * 
 * @jaqlExample denull( [1, null, 3] );
 * [ 1, 3 ]
 */
public class DenullFn extends MacroExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("denull", DenullFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public DenullFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public DenullFn(Expr expr)
  {
    this(new Expr[]{expr});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.MacroExpr#expand(com.ibm.jaql.lang.core.Env)
   */
  @Override
  public Expr expand(Env env) throws Exception
  {
    Var var = env.makeVar("$denull");
    Expr test = new NotExpr(new IsnullExpr(new VarExpr(var)));
    Expr collect = new IfExpr(test, new ArrayExpr(new VarExpr(var)));
    Expr fe = new ForExpr(var, exprs[0], collect);
    return fe;
  }
}
