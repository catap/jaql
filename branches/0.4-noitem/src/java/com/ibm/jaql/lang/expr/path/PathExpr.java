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

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;

public final class PathExpr extends Expr
{
  /**
   * @param exprs
   */
  public PathExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   * @param input
   */
  public PathExpr(Expr input)
  {
    super(input, new PathReturn());
  }

  /**
   * 
   * @param input
   * @param next
   */
  public PathExpr(Expr input, PathStep next)
  {
    super(input, next);
  }

  /**
   * Return the input to the path expression.
   */
  public Expr input()
  {
    return exprs[0];
  }

  /**
   * Return the first step of the path expression.
   */
  public PathStep firstStep()
  {
    return (PathStep)exprs[1];
  }

  /**
   * Return the last step of the path expression.
   */
  public PathStep getReturn() // TODO: Should this be a PathReturn?
  {
    return firstStep().getReturn();
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(")");
    exprs[1].decompile(exprText, capturedVars);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    PathStep s = firstStep();
    s.input = input().eval(context);
    return s.eval(context);
  }

  /**
   * 
   */
  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    PathStep s = firstStep();
    s.input = input().eval(context);
    return s.iter(context);
  }

}
