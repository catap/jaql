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
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.agg.AlgebraicAggregate;

public abstract class AggregateAlgebraicExpr extends AggregateExpr
{
  protected AlgebraicAggregate[] aggs;

  /**
   * @param input
   * @param aggs a list of AlgebraicAggregate
   * @return
   */
  protected static Expr[] makeArgs(BindingExpr input, ArrayList<Expr> aggs)
  {
    int n = aggs.size();
    Expr[] args = new Expr[n+1];
    args[0] = input;
    for(int i = 0 ; i < n ; i++)
    {
      args[i+1] = aggs.get(i);
    }
    return args;
  }

  public AggregateAlgebraicExpr(Expr[] inputs)
  {
    super(inputs);
  }

  /**
   * 
   * @param input
   * @param aggs A list of AlgebraicAggregate
   */
  public AggregateAlgebraicExpr(BindingExpr input, ArrayList<Expr> aggs)
  {
    super(makeArgs(input, aggs));
  }
  
  protected void makeWorkingArea()
  {
    if( aggs == null )
    {
      super.makeWorkingArea();
      int n = numAggs();
      aggs = new AlgebraicAggregate[n];
      for(int i = 0 ; i < n ; i++)
      {
        aggs[i] = (AlgebraicAggregate)agg(i);
      }
    }
    else
    {
      assert numAggs() == aggs.length && numAggs() == tempAggs.length;
    }
  }

  /** Combine partial aggregates
   * 
   * @param context
   * @throws Exception
   */
  public void evalPartial(Context context) throws Exception
  {
    for(int i = 0 ; i < aggs.length ; i++)
    {
      aggs[i].init(context);
    }

    BindingExpr in = binding();
    // in.var.set(tmpItem); // TODO: allow expressions on partial aggregates?
    JsonIterator iter = in.inExpr().iter(context);
    for (JsonValue value : iter)
    {
      JsonArray arr = (JsonArray)value;
      arr.getAll(tempAggs);
      for(int i = 0 ; i < aggs.length ; i++)
      {
        aggs[i].combine(tempAggs[i]);
      }
    }
  }

  protected JsonArray partialResult() throws Exception
  {
    for(int i = 0 ; i < aggs.length ; i++)
    {
      tempAggs[i] = aggs[i].getPartial();
    }
    tuple.set(tempAggs, tempAggs.length);
    return tuple;
  }

}
