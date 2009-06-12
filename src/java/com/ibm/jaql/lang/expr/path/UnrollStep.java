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
package com.ibm.jaql.lang.expr.path;

import java.io.PrintStream;
import java.util.HashSet;

import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;

public abstract class UnrollStep extends Expr // TODO: make PseudoExpr
{

  /**
   * @param exprs
   */
  public UnrollStep(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   * @param expr0
   */
  public UnrollStep(Expr expr0)
  {
    super(expr0);
  }

  public abstract void decompile(PrintStream exprText, HashSet<Var> capturedVars) throws Exception;

  /**
   * 
   * @return The item to expand.  It's value is replaced.
   */
  public abstract JsonHolder expand(Context context, JsonHolder toExpand) throws Exception;

  @Override
  public final JsonValue eval(Context context) throws Exception
  {
    throw new RuntimeException("ExpandStep "+getClass().getName()+" should not be evaluated");
  }
}

