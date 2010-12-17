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
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IndexExpr;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.SortExpr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.JaqlFunction;
import com.ibm.jaql.lang.expr.function.VarParameter;
import com.ibm.jaql.lang.expr.function.VarParameters;

/**
 * @jaqlDescription Reverse an array
 * 
 * Usage:
 * array reverse(array arr)
 * 
 * @jaqlExample range(1,10) -> reverse();
 *  [ 10,9,8,7,6,5,4,3,2,1 ]
 * 
 * @jaqlExample [[0],[1,2],[3,4,5],[6,7,8,9]] -> transform reverse($)->reverse();
 * [ [9,8,7,6] , [5,4,3] , [2,1], [0] ] // reverse sequence
 * 
 *  
 */
public class ReverseFn extends MacroExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("reverse", ReverseFn.class);
    }
  }
  
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
  public Expr expandRaw(Env env) throws Exception
  {
    // exprs[0] -> enumerate() -> sort by ($[0] desc) -> emit $[1] 
    Expr e = new EnumerateExpr(exprs[0]);
    Var v = env.makeVar("$");
    BindingExpr b = new BindingExpr(BindingExpr.Type.IN, v, null, e);
    CmpSingle by = new CmpSingle(new CmpSpec(new IndexExpr(new VarExpr(v), 0), CmpSpec.Order.DESC));
    ConstExpr cmp = new ConstExpr(
        new JaqlFunction(new VarParameters(new VarParameter(v)), by));
    SortExpr sort = new SortExpr(b, cmp);
    b = new BindingExpr(BindingExpr.Type.IN, v, null, sort);
    e = new TransformExpr(b, new IndexExpr(new VarExpr(v), 1));
    return e;
  }
}
