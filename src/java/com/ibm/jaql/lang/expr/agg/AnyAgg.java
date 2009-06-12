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
package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/** Picks any value. If there is at least one non-null values, picks a non-null value.
 * 
 */
@JaqlFn(fnName = "any", minArgs = 1, maxArgs = 1)
public final class AnyAgg extends AlgebraicAggregate
{
  private JsonValue result;
  
  /**
   * Expr aggInput, Expr N
   * @param exprs
   */
  public AnyAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public AnyAgg(Expr expr)
  {
    super(expr);
  }

  @Override
  public void initInitial(Context context) throws Exception
  {
    result = null;
  }

  @Override
  public void addInitial(JsonValue value) throws Exception
  {
    if( result == null  )
    {
      result = value.getCopy(null);
    }
  }

  @Override
  public JsonValue getPartial() throws Exception
  {
    return result;
  }

  @Override
  public void addPartial(JsonValue value) throws Exception
  {
    if( result == null  )
    {
      result = value.getCopy(null);
    }
  }

  @Override
  public JsonValue getFinal() throws Exception
  {
    return result;
  }
}
