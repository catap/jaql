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
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DefineFunctionExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FunctionCallExpr;

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
    DefineFunctionExpr f;
    if (callFn instanceof DefineFunctionExpr)
    {
      f = (DefineFunctionExpr)callFn;
    }
    else if (callFn instanceof ConstExpr)
    {
      ConstExpr c = (ConstExpr) callFn;
      if( !(c.value instanceof JaqlFunction) )
      {
        throw new RuntimeException("function expected, found: "+c.value);
      }
      JaqlFunction jf = (JaqlFunction) c.value;
      f =(DefineFunctionExpr)cloneExpr(jf.getFunction());
    }
    else if( callFn instanceof DoExpr )
    {
      // Push call into DoExpr return:
      //     (..., e2)(e3)
      // ==> (..., e2(e3))
      DoExpr de = (DoExpr)callFn;
      call.replaceInParent(de);
      Expr ret = de.returnExpr();
      ret.replaceInParent(call);
      call.setChild(0, ret);
      return true;
    }
    else
    {
      return false;
    }

    int numParams = f.numParams();
    if (numParams != call.numArgs())
    {
      throw new RuntimeException(
          "invalid number of arguments to function. expected:" + numParams
              + " got:" + call.numArgs());
    }
    
    // TODO: Don't inline (potentially) recursive functions

    // Inline zero-arg function by just its function body. 
    if (numParams == 0)
    {
      call.replaceInParent(f.body());
      return true;
    }

    // For functions with args, create a let to evaluate the args. 
    for (int i = 0; i < numParams; i++)
    {
      f.param(i).addChild(call.arg(i));
    }
    DoExpr let = new DoExpr(f.children());
    call.replaceInParent(let);
    return true;
  }
}
