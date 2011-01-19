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
package com.ibm.jaql.lang.expr.core;

import java.util.ArrayList;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.lang.core.Context;


public class AggregateInitialExpr extends AggregateAlgebraicExpr
{
  // Binding input, AlgebraicAggregate[] aggregates 
  public AggregateInitialExpr(Expr[] inputs)
  {
    super(inputs);
  }
  
  /**
   * @param input
   * @param aggs A list of AlgebraicAggregate
   */
  public AggregateInitialExpr(BindingExpr input, ArrayList<Expr> aggs)
  {
    super(makeArgs(input, aggs));
  }
  
  public AggType getAggType()
  {
    return AggType.INITIAL;
  }

  @Override
  public JsonArray eval(final Context context) throws Exception
  {
    makeWorkingArea();
    evalInitial(context, aggs);
    return partialResult();
  }
}
