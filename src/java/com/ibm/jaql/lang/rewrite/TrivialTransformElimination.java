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
package com.ibm.jaql.lang.rewrite;

import com.ibm.jaql.lang.expr.array.AsArrayFn;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.TransformExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import static com.ibm.jaql.json.type.JsonType.*;

/**
 *     e1 -> transform $ $
 * ==>
 *     asArray(e1)
 * ==>
 *     e1              if e1 is always an array
 * 
 */
public class TrivialTransformElimination extends Rewrite
{
  /**
   * @param phase
   */
  public TrivialTransformElimination(RewritePhase phase)
  {
    super(phase, TransformExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    TransformExpr te = (TransformExpr) expr;
    expr = te.projection();
    if( expr instanceof VarExpr )
    {
      VarExpr ve = (VarExpr)expr;
      BindingExpr b = te.binding();
      if( ve.var() == te.binding().var )
      {
        Expr e = b.inExpr();
        if( e.getSchema().is(ARRAY,NULL).maybeNot() )
        {
          e = new AsArrayFn(e);
        }
        te.replaceInParent(e);
        return true;
      }
    }
    return false;
  }
}
