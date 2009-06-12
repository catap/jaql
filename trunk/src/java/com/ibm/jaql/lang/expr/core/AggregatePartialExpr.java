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

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.agg.AlgebraicAggregate;


public class AggregatePartialExpr extends AggregateAlgebraicExpr
{
  // Binding input, AlgebraicAggregate[] aggregates 
  public AggregatePartialExpr(Expr[] inputs)
  {
    super(inputs);
    onlyTrivialInput();
  }
  
  public AggregatePartialExpr(BindingExpr input, ArrayList<AlgebraicAggregate> aggs)
  {
    super(input, aggs);
    onlyTrivialInput();
  }

  public AggType getAggType()
  {
    return AggType.PARITIAL;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    makeWorkingArea();
    boolean hadInput = evalPartial(context);
    return partialResult(hadInput);
  }

}
