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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.expr.core.Expr;

/** Parameters associated with variables. */
public class VarParameters extends Parameters<Expr>
{
  public VarParameters(VarParameter ... parameters)
  {
    super(parameters);
  }

  @Override
  protected Parameter<Expr> createParameter(JsonString name, Schema schema)
  {
    throw new UnsupportedOperationException(); // not needed here
  }

  @Override
  protected Parameter<Expr> createParameter(JsonString name, Schema schema,
      Expr defaultValue)
  {
    throw new UnsupportedOperationException(); // not needed here
  }

  @Override
  protected Expr[] newArrayOfT(int size)
  {
    return new Expr[size];
  }

  @Override
  public VarParameter get(int i)
  {
    return (VarParameter)super.get(i);
  }
}
