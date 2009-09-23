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

import java.math.BigDecimal;
import java.math.MathContext;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * covStats(array x) = sum [1 x1 x2 ... xn] * [1 x1 x2 ... xn]^T
 *   = [ count   sum(x1)    sum(x2)    ... sum(xn)    ,
 *               sum(x1*x1) sum(x1*x2) ... sum(x1*xn) ,
 *                          sum(x2*x2) ... sum(x2*xn) ,
 *       ...                                          ,
 *                                         sum(xn*xn) ]
 */
public class CovStatsAgg extends AlgebraicAggregate
{
  private JsonValue[]       tuple; // [n]
  private BigDecimal[]   vec;   // [n+1]
  private BigDecimal[][] sum;   // [n+1][n+1]
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("covStats", CovStatsAgg.class);
    }
  }
  
  /**
   * one arg
   * @param exprs
   */
  public CovStatsAgg(Expr[] exprs)
  {
    super(exprs);
  }

  public CovStatsAgg(Expr arg)
  {
    super(arg);
  }
  
  @Override
  public void init(Context context) throws Exception
  {
    tuple = null;
    sum = null;
  }

  @Override
  public void accumulate(JsonValue value) throws Exception
  {
    if( value == null  )
    {
      return;
    }
    JsonArray arr = (JsonArray)value;
    if( tuple == null )
    {
      long nn = arr.count();
      int n = (int)nn;
      tuple = new JsonValue[n];
      vec = new BigDecimal[n+1];
      vec[0] = BigDecimal.ONE;
      sum = new BigDecimal[n+1][n+1];
      for(int i = 0 ; i <= n ; i++)
      {
        for(int j = i ; j <= n ; j++)
        {
          sum[i][j] = BigDecimal.ZERO;
        }
      }
    }
    arr.getAll(tuple);
    for(int i = 0 ; i < tuple.length ; i++)
    {
      JsonNumber num = (JsonNumber)tuple[i];
      BigDecimal d = num.decimalValue();
      vec[i+1] = d;
    }
    for(int i = 0 ; i < vec.length ; i++)
    {
      for(int j = i ; j < vec.length ; j++)
      {
        BigDecimal d = vec[i].multiply(vec[j], MathContext.DECIMAL128);
        sum[i][j] = sum[i][j].add(d, MathContext.DECIMAL128);
      }
    }
  }

  @Override
  public JsonValue getPartial() throws Exception
  {
    BufferedJsonArray arr = new BufferedJsonArray(sum.length * (sum.length + 1) / 2);
    int k = 0;
    for(int i = 0 ; i < sum.length ; i++)
    {
      for(int j = i ; j < sum.length ; j++)
      {
        arr.set(k++, new JsonDecimal(sum[i][j]));
      }
    }
    return arr;
  }

  @Override
  public void combine(JsonValue value) throws Exception
  {
    JsonArray arr = (JsonArray)value;
    if( sum == null )
    {
      // a = n * (n+1) / 2
      // n = floor( sqrt(2a) )
      long nn = arr.count();
      int n = (int)Math.sqrt(nn);
      sum = new BigDecimal[n+1][n+1];
      for(int i = 0 ; i <= n ; i++)
      {
        for(int j = i ; j <= n ; j++)
        {
          sum[i][j] = BigDecimal.ZERO;
        }
      }
    }
    JsonIterator iter = arr.iter();
    for(int i = 0 ; i < sum.length ; i++)
    {
      for(int j = i ; j < sum.length ; j++)
      {
        boolean valid = iter.moveNext();
        assert valid == true;
        JsonNumber num = (JsonNumber)iter.current();
        BigDecimal d = num.decimalValue();
        sum[i][j] = sum[i][j].add(d, MathContext.DECIMAL128);
      }
    }
  }

  @Override
  public JsonValue getFinal() throws Exception
  {
    return getPartial();
  }

}
