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
package com.ibm.jaql.lang.expr.hadoop;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.io.hadoop.JsonHolderMapOutputKey;
import com.ibm.jaql.io.hadoop.JsonHolderMapOutputValue;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JaqlFunction;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.agg.AlgebraicAggregate;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
@JaqlFn(fnName = "mrAggregate", minArgs = 1, maxArgs = 1)
public class MRAggregate extends MapReduceBaseExpr
{

  /**
   * mrAggregate( record args ) { input, output, init, combine, final }
   * 
   * @param exprs
   */
  public MRAggregate(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param args
   */
  public MRAggregate(Expr args)
  {
    this(new Expr[]{args});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(final Context context) throws Exception
  {
    JsonRecord args = baseSetup(context);
    JsonValue map = args.getRequired(new JsonString("map"));
    JsonValue agg = args.getRequired(new JsonString("aggregate"));
    JsonValue finl = args.getRequired(new JsonString("final"));

    // use default: conf.setNumMapTasks(10); // TODO: need a way to specify options
    // use default: conf.setNumReduceTasks(2); // TODO: get from options
    conf.setMapRunnerClass(MapEval.class);
    conf.setCombinerClass(AggCombineEval.class);
    conf.setReducerClass(AggFinalEval.class);

    // setup serialization
    setupSerialization(true);
    JsonValue schema = args.get(new JsonString("schema"));
    if (schema != null) {
      conf.set(SCHEMA_NAME, schema.toString());
    }
    
    JaqlFunction mapFn = JaqlUtil.enforceNonNull((JaqlFunction) map);
    prepareFunction("map", 1, mapFn, 0);
    JaqlFunction aggFn = JaqlUtil.enforceNonNull((JaqlFunction) agg);
    prepareFunction("aggregate", 2, aggFn, 0);
    JaqlFunction finalFn = JaqlUtil.enforceNonNull((JaqlFunction) finl);
    prepareFunction("final", 2, finalFn, 0);

    JobClient.runJob(conf);

    return outArgs;
  }

  protected static AlgebraicAggregate[] makeAggs(JaqlFunction aggFn)
  {
    Expr e = aggFn.getBody();
    if( !(e instanceof ArrayExpr) )
    {
      throw new RuntimeException("aggregate function must start with an array expression");
    }
    int numAggs = e.numChildren();
    AlgebraicAggregate[] aggs = new AlgebraicAggregate[numAggs];
    for(int i = 0 ; i < numAggs ; i++)
    {
      Expr c = e.child(i);
      if( !(c instanceof AlgebraicAggregate) )
      {
        throw new RuntimeException("aggregate function must be an array of AlgebraicAggregate functions");
      }
      aggs[i] = (AlgebraicAggregate)c;
    }
    return aggs;
  }
  
  /**
   * Used for both map and init functions
   */
  public static class MapEval extends RemoteEval
      implements MapRunnable<JsonHolder, JsonHolder, JsonHolderMapOutputKey, JsonHolderMapOutputValue>
  {
    protected JaqlFunction mapFn;
    protected JaqlFunction aggFn;

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr.RemoteEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    @Override
    public void configure(JobConf job)
    {
      super.configure(job);
      mapFn = compile(job, "map", 0);
      aggFn = compile(job, "aggregate", 0);
    }

    /**
     * 
     */
    // fails on java 1.5: @Override
    public void run( RecordReader<JsonHolder, JsonHolder> input,
                     OutputCollector<JsonHolderMapOutputKey, JsonHolderMapOutputValue> output, 
                     Reporter reporter) 
      throws IOException
    {
      // TODO: If there was a way for combiners to:
      //   1. be guaranteed to run at least once
      //   2. know if it is the first invocation
      // then we could avoid initializing aggregates for just one value.
      try
      {
        AlgebraicAggregate[] aggs = makeAggs(aggFn);
        Var keyVar = aggFn.param(0);
        Var valVar = aggFn.param(1);
        JsonValue[] mappedKeyValue = new JsonValue[2];
        BufferedJsonArray aggArray = new BufferedJsonArray(aggs.length);
        JsonHolderMapOutputValue aggArrayHolder = new JsonHolderMapOutputValue(aggArray);
        JsonHolderMapOutputKey keyHolder = new JsonHolderMapOutputKey();
        
        JsonIterator iter = mapFn.iter(context, new RecordReaderValueIter(input));
        BufferedJsonArray tmpArray = new BufferedJsonArray(1);
        for (JsonValue value : iter)
        {
          JsonArray pair = (JsonArray) value;
          if (pair != null)
          {
            pair.getAll(mappedKeyValue);
            keyVar.setValue(mappedKeyValue[0]);
            tmpArray.set(0, mappedKeyValue[1]);
            valVar.setValue(tmpArray);
            for( int i = 0 ; i < aggs.length ; i++ )
            {
              AlgebraicAggregate agg = aggs[i];
              agg.initInitial(context);
              aggs[i].evalInitial(context);
              JsonValue part = agg.getPartial();
              aggArray.set(i, part);
            }
            keyHolder.value = mappedKeyValue[0]; 
            output.collect(keyHolder, aggArrayHolder);
          }
        }
      }
      catch (IOException ex)
      {
        throw ex;
      }
      catch (Exception ex)
      {
        throw new UndeclaredThrowableException(ex);
      }
    }
  }

  /**
   * 
   */
  // produces JsonHolderMapOutputKey and JsonHolderMapOutputValue
  public static class AggCombineEval extends RemoteEval 
  implements  Reducer<JsonHolderMapOutputKey, JsonHolderMapOutputValue, JsonHolder, JsonHolder> 
  {
    protected JaqlFunction aggFn;
    protected Var keyVar;
    protected AlgebraicAggregate[] aggs;
    protected BufferedJsonArray aggArray;
    protected JsonHolder keyHolder;
    protected JsonHolder aggArrayHolder;


    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr.RemoteEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    public void configure(JobConf job)
    {
      super.configure(job);
      aggFn = compile(job, "aggregate", 0);
      if( aggFn.getNumParameters() != 2 )
      {
        throw new RuntimeException("aggregate function must have exactly two parameters");
      }
      keyHolder = new JsonHolderMapOutputKey();
      keyVar = aggFn.param(0);
      aggs = makeAggs(aggFn);
      aggArray = new BufferedJsonArray(aggs.length);
      aggArrayHolder = new JsonHolderMapOutputValue(aggArray);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
     *      java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
     *      org.apache.hadoop.mapred.Reporter)
     */
    public void reduce(JsonHolderMapOutputKey key, Iterator<JsonHolderMapOutputValue> values,
        OutputCollector<JsonHolder, JsonHolder> output, Reporter reporter)
        throws IOException
    {
      try
      {
        for(int i = 0 ; i < aggs.length ; i++)
        {
          aggs[i].initPartial(context);
        }

        keyVar.setValue(key.value);
        keyHolder.value = key.value; // necessary (because key might be a different JsonHolder impl
        
        while( values.hasNext() )
        {
          BufferedJsonArray partArray = (BufferedJsonArray) values.next().value;
          for(int i = 0 ; i < aggs.length ; i++)
          {
            aggs[i].addPartial(partArray.get(i));
          }
        }
        processAggs(keyHolder, output);
      }
      catch (IOException ex)
      {
        throw ex;
      }
      catch (Exception ex)
      {
        throw new UndeclaredThrowableException(ex);
      }
    }

    protected void processAggs(JsonHolder keyHolder, OutputCollector<JsonHolder, JsonHolder> output)
       throws Exception
    {
      for(int i = 0 ; i < aggs.length ; i++)
      {
        JsonValue part = aggs[i].getPartial();
        aggArray.set(i, part);
      }

      output.collect(keyHolder, aggArrayHolder);
    }  
  }

  /**
   * 
   */
  // produces JsonHolder key/values
  public static class AggFinalEval extends AggCombineEval 
  {
    protected JaqlFunction finalFn;
    protected JsonValue[] finalArgs;
    JsonHolder valueHolder = new JsonHolder();
    
    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MRAggregate.AggCombineEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    public void configure(JobConf job)
    {
      super.configure(job);
      finalFn = compile(job, "final", 0);
      finalArgs = new JsonValue[2];
      keyHolder = new JsonHolder(); // final aggregate uses JsonHolder instead of JsonHolderMapOutputKey
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MRAggregate.AggCombineEval#processAggs(com.ibm.jaql.json.type.Item,
     *      org.apache.hadoop.mapred.OutputCollector)
     */
    @Override
    protected void processAggs(JsonHolder keyHolder, OutputCollector<JsonHolder, JsonHolder> output)
       throws Exception
    {
      for(int i = 0 ; i < aggs.length ; i++)
      {
        JsonValue value = aggs[i].getFinal();
        aggArray.set(i, value);
      }

      finalArgs[0] = keyHolder.value;
      finalArgs[1] = aggArray;
      JsonIterator iter = finalFn.iter(context, finalArgs);
      
      for (JsonValue value : iter) {
        valueHolder.value = value;
        output.collect(keyHolder, valueHolder);
      }
    }
  }
}
