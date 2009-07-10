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
package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "count", minArgs = 1, maxArgs = 1)
public final class CountAgg extends AlgebraicAggregate
{
  private long count;
  
  /**
   * @param exprs
   */
  public CountAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public CountAgg(Expr expr)
  {
    super(expr);
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonIterator iter = exprs.length == 0 ? JsonIterator.EMPTY : exprs[0].iter(context);
    count = 0;

    while( iter.moveNext() )
    {
      count++;
    }

    return new JsonLong(count);
  }

  @Override
  public void initInitial(Context context) throws Exception
  {
    count = 0;
  }

  @Override
  public void addInitial(JsonValue value) throws Exception
  {
    count++;
  }

  @Override
  public JsonValue getPartial() throws Exception
  {
    return new JsonLong(count);
  }

  @Override
  public void addPartial(JsonValue value) throws Exception
  {
    JsonLong n = (JsonLong)value;
    count += n.get();
  }

  @Override
  public JsonValue getFinal() throws Exception
  {
    return new JsonLong(count);
  }
}
