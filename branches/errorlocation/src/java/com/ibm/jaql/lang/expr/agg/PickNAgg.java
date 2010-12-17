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

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription select N elements from an array
 * Usage:
 * [T] pickN( [T], long n );
 * 
 * Select n elements from the input array.
 * 
 * @jaqlExample pickN( [1,2,3], 2 )
 * [1,2]
 */
public final class PickNAgg extends AlgebraicAggregate // TODO: should this preserve nulls?
{
  private SpilledJsonArray array = new SpilledJsonArray();
  
  private long limit;
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("pickN", PickNAgg.class);
    }
  }
  
  /**
   * Expr aggInput, Expr N
   * @param exprs
   */
  public PickNAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public PickNAgg(Expr expr)
  {
    super(expr);
  }

  @Override
  public void init(Context context) throws Exception
  {
    array.clear();
    JsonNumber num = (JsonNumber)exprs[1].eval(context);
    limit = num.longValueExact();
  }

  @Override
  public void accumulate(JsonValue value) throws Exception
  {
    if( value == null  )
    {
      return;
    }
    if( array.count() < limit )
    {
      array.addCopy(value);
    }
  }

  @Override
  public JsonValue getPartial() throws Exception
  {
    return array;
  }

  @Override
  public void combine(JsonValue value) throws Exception
  {
    JsonArray array2 = (JsonArray)value;
    long m = array.count();
    long n = array2.count();
    JsonIterator iter = array2.iter();
    if( m + n < limit )
    {
      array.addCopyAll(iter);
    }
    else
    {
      for( ; m < limit ; m++ )  
      {
        boolean valid = iter.moveNext();
        assert valid;
        array.addCopy(iter.current()); // TODO: we will find all of them
      }
    }
  }

  @Override
  public JsonValue getFinal() throws Exception
  {
    return getPartial();
  }
}
