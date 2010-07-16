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
package com.ibm.jaql.lang.expr.top;

import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;

/** 
 * An expression that knows the compile-time environment.
 */
public abstract class EnvExpr extends TopExpr
{
  protected Env env;
  
  public EnvExpr(Env env, Expr ... exprs)
  {
    super(exprs);
    if (env == null) throw new NullPointerException("env must not be null");
    this.env = env;
  }
  
  public Env getEnv()
  {
    return env;
  }
}
