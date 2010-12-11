/*
 * Copyright (C) IBM Corp. 2010.
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

import java.util.Comparator;
import java.util.PriorityQueue;

import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JsonComparator;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.CmpExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.JaqlFunction;
import com.ibm.jaql.util.Pair;

/**
 * @jaqlDescription compute the top N values from an array
 * Usage:
 * [T] topN( [T], long n, cmp(x) );
 * 
 * Given an input array, a limit n, and a comparator function, compute the top n elements
 * of the input array. This implementation uses a heap to efficiently use memory and lower
 * the network traffic that is needed for aggregation.
 * 
 * @jaqlExample [1,2,3] -> write(hdfs("test1"));
 * 
 * @jaqlExample read(hdfs("test1")) -> topN( 2, cmp(x) [x desc ] ); // Simplest example
 * [ 1, 2 ]
 * 
 * @jaqlExample read(hdfs("test1")) -> group into topN( $, 2, cmp(x) [ x desc ] ); // Now, with group by (this uses map-reduce)
 * 
 * @jaqlExample [ [ 1, 1 ], [1, 2], [2, 0], [2, 11], [3, 3], [3, 4], [3, 5] ] -> write(hdfs("test2"));
 * 
 * @jaqlExample read(hdfs("test2")) -> group by n = $[0] into { num: n, top: topN($[*][1], 1, cmp(x) [ x desc ]) }; // Complex data
 */
public final class TopNAgg extends AlgebraicAggregate
{
  protected int limit;
  protected Var cmpVar;
  protected CmpExpr cmpExpr;
  protected JsonComparator jcmp; 
  protected PriorityQueue<Pair<JsonHolder,JsonValue>> topVals;
  protected Context context;
  protected JsonHolder tmpHolder;
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par33 // TODO: make cmp optional; default=$ desc
  {
    public Descriptor()
    {
      super("topN", TopNAgg.class);
    }
  }
  
  /**
   * @param exprs
   */
  public TopNAgg(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public void init(Context context) throws Exception
  {
    this.context = context;
    
    JsonNumber jnum = (JsonNumber)exprs[1].eval(context);
    limit = jnum.intValue();
    
    // TODO: This is common to SortExpr; comparators need to be cleaned up...
    JaqlFunction fn = (JaqlFunction)exprs[2].eval(context);
    if( fn.getParameters().numParameters() != 1 || !(fn.body() instanceof CmpExpr) )
    {
      throw new RuntimeException("invalid comparator function");
    }
    
    cmpVar = fn.getParameters().get(0).getVar();
    cmpExpr = (CmpExpr)fn.body();
    jcmp = cmpExpr.getComparator(context);
    
    Comparator<Pair<JsonHolder,JsonValue>> cmp = new Comparator<Pair<JsonHolder,JsonValue>>()
    {
      @Override
      public int compare( Pair<JsonHolder, JsonValue> x,
                          Pair<JsonHolder, JsonValue> y)
      {
        // Comparison inverted because we want the smallest value at the head of the queue.
        return jcmp.compare(y.a,x.a);
      }
    };
    
    topVals = new PriorityQueue<Pair<JsonHolder,JsonValue>>(limit,cmp);
    
    tmpHolder = new JsonHolder();
  }

  @Override
  public void accumulate(JsonValue value) throws Exception
  {
    if( value != null )
    {
      cmpVar.setValue(value);
      JsonValue byValue = cmpExpr.eval(context);
      if( topVals.size() < limit )
      {
        topVals.add( 
            new Pair<JsonHolder,JsonValue>(
              new JsonHolder(byValue.getCopy(null)),
              value.getCopy(null))
           );
      }
      else 
      {
        tmpHolder.value = byValue;
        if( jcmp.compare( tmpHolder, topVals.peek().a) < 0 )
        {
          Pair<JsonHolder,JsonValue> reuse = topVals.remove();
          reuse.a.value = byValue.getCopy(reuse.a.value);
          reuse.b = value.getCopy(reuse.b);
          topVals.add( reuse );
        }
      }
    }
  }

  @Override
  public JsonValue getPartial() throws Exception
  {
    JsonValue[] vs = new JsonValue[topVals.size()];
    
    // This returns the top values in any order. 
    //    int i = 0;
    //    for( Pair<JsonHolder, JsonValue> keyVal: topVals )
    //    {
    //      vs[i++] = keyVal.b;
    //    }
    
    // This returns the values in the order of the comparator.
    for( int i = vs.length-1 ; i >= 0 ; i-- )
    {
      vs[i] = topVals.remove().b;
    }
    
    return new BufferedJsonArray(vs, vs.length, false);
  }

  @Override
  public void combine(JsonValue value) throws Exception
  {
    // TODO: this could be made faster by exploiting the fact that
    // each combined value is sorted and its size is <= limit.
    JsonArray vs = (JsonArray)value;
    for(JsonValue v: vs)
    {
      accumulate(v);
    }
  }

  @Override
  public Schema getPartialSchema()
  {
    return exprs[0].getSchema();
  }
  
  @Override
  public JsonValue getFinal() throws Exception
  {
    return getPartial();
  }
  
  @Override
  public Schema getSchema()
  {
    return exprs[0].getSchema();
  }

}
