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

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.agg.AlgebraicAggregate;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

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
  public Item eval(final Context context) throws Exception
  {
    JRecord args = baseSetup(context);
    Item map = args.getRequired("map");
    Item agg = args.getRequired("aggregate");
    Item finl = args.getRequired("final");

    // use default: conf.setNumMapTasks(10); // TODO: need a way to specify options
    // use default: conf.setNumReduceTasks(2); // TODO: get from options
    conf.setMapRunnerClass(MapEval.class);
    conf.setCombinerClass(AggCombineEval.class);
    conf.setReducerClass(AggFinalEval.class);

    JFunction mapFn = (JFunction) map.getNonNull();
    prepareFunction("map", 1, mapFn, 0);
    JFunction aggFn = (JFunction) agg.getNonNull();
    prepareFunction("aggregate", 2, aggFn, 0);
    JFunction finalFn = (JFunction) finl.getNonNull();
    prepareFunction("final", 2, finalFn, 0);

    JobClient.runJob(conf);

    return outArgs;
  }

  protected static AlgebraicAggregate[] makeAggs(JFunction aggFn)
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
      implements MapRunnable<Item, Item, Item, Item>
  {
    protected JFunction mapFn;
    protected JFunction aggFn;

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
    public void run( RecordReader<Item, Item> input,
                     OutputCollector<Item, Item> output, 
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
        Item[] mappedKeyValue = new Item[2];
        FixedJArray aggArray = new FixedJArray(aggs.length);
        Item aggItem = new Item(aggArray);

        mapFn.param(0).set(new RecordReaderValueIter(input));
        Iter iter = mapFn.iter(context);

        FixedJArray tmpArray = new FixedJArray(1);
        Item tmpItem = new Item(tmpArray);

        Item item;
        while ((item = iter.next()) != null)
        {
          JArray pair = (JArray) item.get();
          if (pair != null)
          {
            pair.getTuple(mappedKeyValue);
            keyVar.set(mappedKeyValue[0]);
            tmpArray.set(0, mappedKeyValue[1]);
            valVar.set(tmpItem);
            for( int i = 0 ; i < aggs.length ; i++ )
            {
              AlgebraicAggregate agg = aggs[i];
              agg.initInitial(context);
              aggs[i].evalInitial(context);
              Item part = agg.getPartial();
              aggArray.set(i, part);
            }
            output.collect(mappedKeyValue[0], aggItem);
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
  public static class AggCombineEval extends RemoteEval
      implements
        Reducer<Item, Item, Item, Item>
  {
    protected JFunction aggFn;
    protected Var keyVar;
    protected AlgebraicAggregate[] aggs;
    protected FixedJArray aggArray;
    protected Item aggItem;


    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr.RemoteEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    public void configure(JobConf job)
    {
      super.configure(job);
      aggFn = compile(job, "aggregate", 0);
      keyVar = aggFn.param(0);
      aggs = makeAggs(aggFn);
      aggArray = new FixedJArray(aggs.length);
      aggItem = new Item(aggArray);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
     *      java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
     *      org.apache.hadoop.mapred.Reporter)
     */
    public void reduce(Item key, Iterator<Item> values,
        OutputCollector<Item, Item> output, Reporter reporter)
        throws IOException
    {
      try
      {
        for(int i = 0 ; i < aggs.length ; i++)
        {
          aggs[i].initPartial(context);
        }

        keyVar.set(key);
        
        while( values.hasNext() )
        {
          Item parts = values.next();
          FixedJArray partArray = (FixedJArray)parts.get();
          for(int i = 0 ; i < aggs.length ; i++)
          {
            aggs[i].addPartial(partArray.get(i));
          }
        }
        processAggs(key, output);
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

    protected void processAggs(Item key, OutputCollector<Item, Item> output)
       throws Exception
    {
      for(int i = 0 ; i < aggs.length ; i++)
      {
        Item part = aggs[i].getPartial();
        aggArray.set(i, part);
      }

      output.collect(key, aggItem);
    }  
  }

  /**
   * 
   */
  public static class AggFinalEval extends AggCombineEval
  {
    protected JFunction finalFn;
    protected Item[]    finalArgs;

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MRAggregate.AggCombineEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    public void configure(JobConf job)
    {
      super.configure(job);
      finalFn = compile(job, "final", 0);
      finalArgs = new Item[2];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MRAggregate.AggCombineEval#processAggs(com.ibm.jaql.json.type.Item,
     *      org.apache.hadoop.mapred.OutputCollector)
     */
    @Override
    protected void processAggs(Item key, OutputCollector<Item, Item> output)
       throws Exception
    {
      for(int i = 0 ; i < aggs.length ; i++)
      {
        Item item = aggs[i].getFinal();
        aggArray.set(i, item);
      }

      finalArgs[0] = key;
      finalArgs[1] = aggItem;
      Iter iter = finalFn.iter(context, finalArgs);
      Item item;
      while( (item = iter.next()) != null )
      {
        output.collect(key, item);
      }
    }
  }
}
