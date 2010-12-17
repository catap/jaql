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

import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.path.PathExpr;

/**
 * Rewrite the first step of a path expression.  The transformation depends
 * on the particular type of PathStep.
 * 
 * @see PathStep.rewriteFirstStep()
 */
public class RewriteFirstPathStep extends Rewrite
{
  /**
   * @param phase
   */
  public RewriteFirstPathStep(RewritePhase phase)
  {
    super(phase, PathExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    PathExpr pe = (PathExpr)expr;
    return pe.firstStep().rewriteFirstStep();
  }
}
