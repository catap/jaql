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
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.Bool3;


public abstract class Aggregate extends Expr
{
  /** Most aggregates have a single argument.
   * 
   * @param exprs
   */
  public Aggregate(Expr[] exprs)
  {
    super(exprs);
  }
  
  public Aggregate(boolean overall, Expr arg)
  {
    super(arg);
  }
  
  public Aggregate(Expr arg)
  {
    super(arg);
  }  

  /**
   * 
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    if( i == 0 )
    {
      return Bool3.TRUE;
    }
    return Bool3.FALSE;
  }

  /**
   * This function is called by the AggregateExpr for computing
   * many aggregates simultaneously.  The argument expression
   * is the entire array to aggregate.
   * 
   * The default way to process the input aggregation set is to
   *    for each item in input
   *     - if item is null
   *         - ignore the item
   *     - otherwise add the item
   * 
   * If you override this function, you need to override processInitial as well.
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonIterator iter = exprs[0].iter(context);
    initInitial(context);

    for (JsonValue value : iter) {
      if( value != null )
      {
        addInitial(value);
      }
    }

    return getFinal();
  }

  
  /**
   * This function is called by the AggregateExpr for computing
   * many aggregates simultaneously.  The argument expression
   * is per aggregation row.
   * 
   * The default way to process one aggregation item is to
   *     - if it produces null
   *         - ignore the item
   *     - otherwise add the item
   * 
   * If you override this, you need to override eval() as well.
   *          
   * @param context
   * @param scaler
   * @throws Exception
   */
  public void evalInitial(Context context) throws Exception
  {
    JsonIterator iter = exprs[0].iter(context);
    for (JsonValue value : iter) {
      addInitial(value);
    }
  }

  public abstract void initInitial(Context context) throws Exception;
  public abstract void addInitial(JsonValue value) throws Exception;
  public abstract JsonValue getFinal() throws Exception;
}
