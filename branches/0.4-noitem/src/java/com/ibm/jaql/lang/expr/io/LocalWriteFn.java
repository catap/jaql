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
package com.ibm.jaql.lang.expr.io;

import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "localWrite", minArgs = 2, maxArgs = 2)
public class LocalWriteFn extends AbstractWriteExpr
{
  /**
   * @param exprs
   */
  public LocalWriteFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param toWrite
   * @param fd
   */
  public LocalWriteFn(Expr toWrite, Expr fd)
  {
    super(fd, toWrite);
  }

}
