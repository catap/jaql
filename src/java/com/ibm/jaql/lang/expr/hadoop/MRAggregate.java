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
import org.apache.hadoop.util.ReflectionUtils;

import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.agg.AlgebraicAggregate;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.FunctionCallExpr;
import com.ibm.jaql.lang.expr.function.JaqlFunction;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
public class MRAggregate extends MapReduceBaseExpr
{
  public final static JsonString AGGREGATE_KEY = new JsonString("aggregate");
  public final static JsonString FINAL_KEY = new JsonString("final");
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("mrAggregate", MRAggregate.class);
    }
  }
  
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

  @Override
  public Schema getSchema()
  {
    Schema in = exprs[0].getSchema();
    Schema out = in.element(OUTPUT_KEY);
    return out != null ? out : SchemaFactory.anySchema();
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(final Context context) throws Exception
  {
    JsonRecord args = baseSetup(context);
    JsonValue map = args.getRequired(MAP_KEY);
    JsonValue agg = args.getRequired(AGGREGATE_KEY);
    JsonValue finl = args.getRequired(FINAL_KEY);

    // use default: conf.setNumMapTasks(10); // TODO: need a way to specify options
    // use default: conf.setNumReduceTasks(2); // TODO: get from options
    conf.setMapRunnerClass(MapEval.class);
    conf.setCombinerClass(AggCombineEval.class);
    conf.setReducerClass(AggFinalEval.class);

    // setup serialization
    setupSerialization(true);
    JsonValue schema = args.get(SCHEMA_KEY);
    if (schema != null) {
      conf.set(SCHEMA_NAME, schema.toString());
    }
    
    Function mapFn = JaqlUtil.enforceNonNull((Function) map);
    prepareFunction("map", 1, mapFn, 0);
    JaqlFunction aggFn = JaqlUtil.enforceNonNull((JaqlFunction) agg);
    prepareFunction("aggregate", 2, aggFn, 0);
    JaqlFunction finalFn = JaqlUtil.enforceNonNull((JaqlFunction) finl);
    prepareFunction("final", 2, finalFn, 0);

    //JobClient.runJob(conf);
    Util.submitJob(new JsonString(MRAggregate.class.getName()), conf);

    return outArgs;
  }

  protected static AlgebraicAggregate[] makeAggs(JaqlFunction aggFn)
  {
    assert aggFn.getLocalBindings().isEmpty(); // has been inlined in configure(...)
    Expr e = aggFn.body();
    if( !(e instanceof ArrayExpr) )
    {
      throw new RuntimeException("aggregate function must start with an array expression");
    }
    int numAggs = e.numChildren();
    AlgebraicAggregate[] aggs = new AlgebraicAggregate[numAggs];
    for(int i = 0 ; i < numAggs ; i++)
    {
      Expr c;
      try
      {
        c = FunctionCallExpr.inlineIfPossible(e.child(i));
      }
      catch (Exception ex)
      {
        throw JaqlUtil.rethrow(ex);
      }
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
      implements MapRunnable<JsonHolder, JsonHolder, JsonHolder, JsonHolder>
  {
    protected Function mapFn;
    protected JaqlFunction aggFn;
    JsonHolder aggArrayHolder;
    JsonHolder keyHolder;
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
      aggFn = (JaqlFunction)compile(job, "aggregate", 0);
      aggFn = aggFn.inlineLocalBindings();
      keyHolder = (JsonHolder)ReflectionUtils.newInstance(job.getMapOutputKeyClass(), job);
      aggArrayHolder = (JsonHolder)ReflectionUtils.newInstance(job.getMapOutputValueClass(), job);
    }

    /**
     * 
     */
    // fails on java 1.5: @Override
    public void run( RecordReader<JsonHolder, JsonHolder> input,
                     OutputCollector<JsonHolder, JsonHolder> output, 
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
        Var keyVar = aggFn.getParameters().get(0).getVar();
        Var valVar = aggFn.getParameters().get(1).getVar();
        JsonValue[] mappedKeyValue = new JsonValue[2];
        BufferedJsonArray aggArray = new BufferedJsonArray(aggs.length);
        aggArrayHolder.value = aggArray;
        mapFn.setArguments(new RecordReaderValueIter(input));
        JsonIterator iter = mapFn.iter(context);
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
              agg.init(context);
              aggs[i].evalInitialized(context);
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
      finally
      {
        this.close();
      }
    }
  }

  /**
   * 
   */
  // produces JsonHolderMapOutputKey and JsonHolderMapOutputValue
  public static class AggCombineEval extends RemoteEval 
  implements  Reducer<JsonHolder, JsonHolder, JsonHolder, JsonHolder> 
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
      aggFn = (JaqlFunction)compile(job, "aggregate", 0);
      aggFn = aggFn.inlineLocalBindings();
      if( aggFn.getParameters().numParameters() != 2 )
      {
        throw new RuntimeException("aggregate function must have exactly two parameters");
      }
      keyHolder = (JsonHolder)ReflectionUtils.newInstance(job.getMapOutputKeyClass(), job);
      keyVar = aggFn.getParameters().get(0).getVar();
      aggs = makeAggs(aggFn);
      aggArray = new BufferedJsonArray(aggs.length);      
      aggArrayHolder = (JsonHolder)ReflectionUtils.newInstance(job.getMapOutputValueClass(), job);
      aggArrayHolder.value = aggArray;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
     *      java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
     *      org.apache.hadoop.mapred.Reporter)
     */
    public void reduce(JsonHolder key, Iterator<JsonHolder> values,
        OutputCollector<JsonHolder, JsonHolder> output, Reporter reporter)
        throws IOException
    {
      try
      {
        for(int i = 0 ; i < aggs.length ; i++)
        {
          aggs[i].init(context);
        }

        keyVar.setValue(key.value);
        keyHolder.value = key.value; // necessary (because key might be a different JsonHolder impl
        
        while( values.hasNext() )
        {
          JsonArray partArray = (JsonArray) values.next().value;
          for(int i = 0 ; i < aggs.length ; i++)
          {
            aggs[i].combine(partArray.get(i));
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
    JsonHolder valueHolder;
    
    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MRAggregate.AggCombineEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    public void configure(JobConf job)
    {
      super.configure(job);
      finalFn = (JaqlFunction)compile(job, "final", 0);
      finalArgs = new JsonValue[2];
      
      Object kTmp = ReflectionUtils.newInstance(job.getOutputKeyClass(), job);
      if( kTmp instanceof JsonHolder )
	keyHolder = (JsonHolder)kTmp;
      else
	keyHolder = new JsonHolder(); // a converter must be in the loop
      
      Object vTmp = ReflectionUtils.newInstance(job.getOutputValueClass(), job);
      if( vTmp instanceof JsonHolder )
	valueHolder = (JsonHolder)vTmp;
      else
	valueHolder = new JsonHolder(); // a converter must be in the loop
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
      finalFn.setArguments(finalArgs, 0, finalArgs.length);
      JsonIterator iter = finalFn.iter(context);
      
      for (JsonValue value : iter) {
        valueHolder.value = value;
        output.collect(keyHolder, valueHolder);
      }
    }
  }
}
