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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.FieldExpr;

/**
 * 
 */
public class ConstEval extends Rewrite
{
  /**
   * @param phase
   */
  public ConstEval(RewritePhase phase)
  {
    super(phase, Expr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    // TODO: we need to be careful computing small functions that produce large results.
    // For now such functions mark themselves as non-constant. Is that the best way?
    if (expr instanceof ConstExpr || expr instanceof BindingExpr
        || expr instanceof FieldExpr || !expr.isConst())
    {
      return false;
    }

    Context context = new Context(); // TODO: memory
    Item item = expr.eval(context);
    ConstExpr c = new ConstExpr(item);
    expr.replaceInParent(c);
    context.endQuery(); // TODO: need to wrap up parse, eval, cleanup into one class and use everywhere
    return true;
  }
}
