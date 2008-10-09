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
package com.ibm.jaql.lang.walk;

import com.ibm.jaql.lang.expr.core.Expr;

/**
 * 
 */
public class OneExprWalker extends ExprWalker
{
  Expr    start;
  boolean done;

  /**
   * 
   */
  public OneExprWalker()
  {
  }

  /**
   * @param start
   */
  public OneExprWalker(Expr start)
  {
    reset(start);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.walk.ExprWalker#reset()
   */
  public void reset()
  {
    done = false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.walk.ExprWalker#reset(com.ibm.jaql.lang.expr.core.Expr)
   */
  public void reset(Expr start)
  {
    this.start = start;
    done = false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.walk.ExprWalker#next()
   */
  public Expr next()
  {
    if (done)
    {
      return null;
    }
    done = true;
    return start;
  }
}
