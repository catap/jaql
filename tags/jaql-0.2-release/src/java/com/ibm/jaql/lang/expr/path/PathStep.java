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

import java.util.ArrayList;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;

public abstract class PathStep extends Expr
{

  /**
   * @param exprs
   */
  public PathStep(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr0
   */
  public PathStep(Expr expr0)
  {
    super(expr0);
  }

  /**
   * @param expr0
   * @param expr1
   */
  public PathStep(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }

  /**
   * @param expr0
   * @param expr1
   */
  public PathStep(Expr expr0, Expr expr1, Expr expr2)
  {
    super(expr0, expr1, expr2);
  }

  /**
   * 
   * @param exprs
   */
  public PathStep(ArrayList<PathStep> exprs)
  {
    super(exprs);
  }

  /**
   * Set the next step of the path expression
   * @param next
   */
  public void setNext(Expr next)
  {
    setChild(exprs.length-1, next);
  }
  
  /**
   * 
   * @return
   */
  public PathStep nextStep()
  {
    return (PathStep)exprs[exprs.length-1];
  }

  /**
   * Make path.name into path{.name}
   */
  public void forceRecord()
  {
    nextStep().forceRecord();
  }

  /**
   * Get the last step.
   */
  public PathStep getReturn()
  {
    PathStep s = this;
    PathStep n = s.nextStep();
    while( n != null )
    {
      s = n;
      n = s.nextStep();
    }
    return s;
  }

  /**
   * 
   * @param context
   * @param input
   * @return
   * @throws Exception
   */
  protected Item nextStep(Context context, Item input) throws Exception
  {
    context.pathInput = input;
    return nextStep().eval(context);
  }
}
