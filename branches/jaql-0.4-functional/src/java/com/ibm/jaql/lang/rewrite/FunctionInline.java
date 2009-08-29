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

import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FunctionCallExpr;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.util.JaqlUtil;

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
    int length = call.numChildren()-1;

    // rewrite when we know the function at compile time
    if (callFn.isCompileTimeComputable().always())
    {
      try
      {
        Function function = (Function)callFn.eval(Env.getCompileTimeContext());
        function = (Function)function.getImmutableCopy();
        function.setArguments(call.children(), 1, length, true);
        Expr inlinedFunction = function.inline();
        call.replaceInParent(inlinedFunction);
        return true;
      }
      catch (Exception e)
      {
        JaqlUtil.rethrow(e);
      }
    }
    
    // rewrite do expressiosn
    if (callFn instanceof DoExpr )
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
    
    return false;
  }
}
