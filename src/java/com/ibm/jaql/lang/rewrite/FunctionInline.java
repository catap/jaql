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

import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FunctionCallExpr;
import com.ibm.jaql.lang.expr.core.LetExpr;

/**
 * Compose a function definition and a function call.
 * 
 * (fn($i,...) { ebody($i,...) })(earg,...) ==> let $i = earg, ... return
 * ebody($i,...)
 * 
 */
public class FunctionInline extends Rewrite
{
  /**
   * @param phase
   */
  public FunctionInline(RewritePhase phase)
  {
    super(phase, FunctionCallExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    FunctionCallExpr call = (FunctionCallExpr) expr;
    Expr callFn = call.fnExpr();
    Expr fnBody;
    Var[] params;
    if (callFn instanceof DefineFunctionExpr)
    {
      DefineFunctionExpr fn = (DefineFunctionExpr) callFn;
      fnBody = fn.body();
      params = fn.params();
    }
    else if (callFn instanceof ConstExpr)
    {
      ConstExpr c = (ConstExpr) callFn;
      if (!(c.value.get() instanceof JFunction))
      {
        // TODO: throw compile-time type-check error
        return false;
      }
      JFunction fn = (JFunction) c.value.get();
      fnBody = cloneExpr(fn.getBody());
      params = new Var[fn.getNumParameters()];
      for (int i = 0; i < params.length; i++)
      {
        Var v = fn.param(i);
        params[i] = engine.varMap.get(v);
      }
    }
    else
    {
      return false;
    }

    int numParams = call.numArgs();
    if (numParams != params.length)
    {
      throw new RuntimeException(
          "invalid number of arguments to function. expected:" + params.length
              + " got:" + numParams);
    }

    // Inline zero-arg function by just its function body. 
    if (numParams == 0)
    {
      call.replaceInParent(fnBody);
      return true;
    }

    // For functions with args, create a let to evaluate the args. 
    ArrayList<BindingExpr> bindings = new ArrayList<BindingExpr>();
    for (int i = 0; i < numParams; i++)
    {
      bindings.add(new BindingExpr(BindingExpr.Type.EQ, params[i], null, call
          .arg(i)));
    }
    LetExpr let = new LetExpr(bindings, fnBody);

    call.replaceInParent(let);
    return true;
  }
}
