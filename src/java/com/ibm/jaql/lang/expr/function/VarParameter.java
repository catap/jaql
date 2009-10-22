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
package com.ibm.jaql.lang.expr.function;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;

/** An implementation of {@link Parameter} associated with a variable. */
public class VarParameter extends Parameter<Expr>
{
  Var var;
  Expr defaultValue;
  
  public VarParameter(Var var, Expr defaultValue)
  {
    super(var.name(), var.getSchema(), defaultValue);
    this.var = var;
    this.defaultValue = defaultValue;
  }
  
  public VarParameter(Var var)
  {
    super(var.name(), var.getSchema());
    this.var = var;
    this.defaultValue = null;
  }

  @Override
  protected Expr processDefault(Expr value)
  {
    return value;
  }
  
  public Var getVar()
  {
    return var;
  }
}
