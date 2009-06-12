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
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "mapReduce", minArgs = 1, maxArgs = 1)
public class MapReduceFn extends MapReduceBaseExpr
{

  /**
   * mapReduce( record args ) { input, output, map, reduce }
   * 
   * @param exprs
   */
  public MapReduceFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param args
   */
  public MapReduceFn(Expr args)
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
    Item combine = args.getValue("combine", null);
    Item reduce = args.getValue("reduce", null);

    conf.setMapperClass(MapEval.class);
    // use default: conf.setNumMapTasks(10); // TODO: need a way to specify options

    if (combine != null)
    {
      if (reduce == null)
      {
        throw new RuntimeException(
            "reduce function required when combine function is specified");
      }
      conf.setCombinerClass(CombineEval.class);
    }

    if (reduce != null)
    {
      // conf.setNumReduceTasks(2); // TODO: get from options
      conf.setReducerClass(ReduceEval.class);
      JFunction reduceFn = (JFunction) reduce.getNonNull();
      prepareFunction("reduce", numInputs + 1, reduceFn, 0);
    }
    else
    {
      conf.setNumReduceTasks(0);
    }

    if (numInputs == 1)
    {
      JFunction mapFn = (JFunction) map.getNonNull();
      prepareFunction("map", 1, mapFn, 0);
      if (combine != null)
      {
        JFunction combineFn = (JFunction) combine.getNonNull();
        prepareFunction("combine", 2, combineFn, 0);
      }
    }
    else
    {
      JArray mapArray = (JArray) map.getNonNull();
      Iter iter = mapArray.iter();
      for (int i = 0; i < numInputs; i++)
      {
        JFunction mapFn = (JFunction) iter.next().getNonNull();
        prepareFunction("map", 1, mapFn, i);
      }
      if (combine != null)
      {
        JArray combineArray = (JArray) combine.getNonNull();
        iter = combineArray.iter();
        for (int i = 0; i < numInputs; i++)
        {
          JFunction combineFn = (JFunction) iter.next().getNonNull();
          prepareFunction("combine", 2, combineFn, i);
        }
      }
    }

    // Uncomment to run locally in a single process
    // conf.set("mapred.job.tracker", (Object)"local");

    JobClient.runJob(conf);

    return outArgs;
  }

  /**
   * 
   */
  public static abstract class CombineReduceEval extends RemoteEval
  {
    protected SpillJArray[] valArrays;

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr.RemoteEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    public void configure(JobConf job)
    {
      super.configure(job);
      valArrays = new SpillJArray[numInputs];
      for (int i = 0; i < numInputs; i++)
      {
        valArrays[i] = new SpillJArray();
      }
    }

    /**
     * @param values
     * @throws IOException
     */
    protected void splitValues(Iterator<Item> values) throws IOException
    {
      // TODO: Would like values to be something that I can open an iterator on. 
      // Until I do the analysis that says that we are going over the values just once,
      // we need to copy the values...
      // TODO: need to reduce copying, big time!
      for (int i = 0; i < numInputs; i++)
      {
        valArrays[i].clear();
      }
      while (values.hasNext())
      {
        Item itemVal = values.next();
        int i = 0;
        if (numInputs > 1)
        {
          FixedJArray valRec = (FixedJArray) itemVal.get();
          JLong id = (JLong) valRec.get(0).getNonNull();
          i = (int) id.value;
          itemVal = valRec.get(1);
        }
        valArrays[i].add(itemVal);
      }
      for (int i = 0; i < numInputs; i++)
      {
        valArrays[i].freeze();
      }
    }
  }

  /**
   * 
   */
  public static class CombineEval extends CombineReduceEval
      implements
        Reducer<Item, Item, Item, Item>
  {
    protected JFunction[] combineFns;
    protected Item[]      fnArgs = new Item[2];
    protected Item[]      valItems;
    protected JLong       outId;
    protected FixedJArray outPair;
    protected Item        outItem;

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MapReduceFn.CombineReduceEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    public void configure(JobConf job)
    {
      super.configure(job);
      combineFns = new JFunction[numInputs];
      valItems = new Item[numInputs];
      for (int i = 0; i < numInputs; i++)
      {
        combineFns[i] = compile(job, "combine", i);
        valItems[i] = new Item(valArrays[i]);
      }
      if (numInputs > 1)
      {
        // FIXME: ideally we could know which input was used to when reading map/combine output files without encoding it on every record
        outId = new JLong();
        outPair = new FixedJArray(2);
        outPair.set(0, new Item(outId));
        outItem = new Item(outPair);
      }
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
        splitValues(values);
        fnArgs[0] = key;
        if (numInputs == 1)
        {
          fnArgs[1] = valItems[0];
          Item item;
          Iter iter = combineFns[0].iter(context, fnArgs);
          while ((item = iter.next()) != null)
          {
            output.collect(key, item);
          }
        }
        else
        {
          for (int i = 0; i < numInputs; i++)
          {
            fnArgs[1] = valItems[i];
            Item item;
            Iter iter = combineFns[i].iter(context, fnArgs);
            while ((item = iter.next()) != null)
            {
              outId.value = i;
              outPair.set(1, item);
              output.collect(key, outItem);
            }
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
  public static class ReduceEval extends CombineReduceEval
      implements
        Reducer<Item, Item, Item, Item>
  {
    protected JFunction reduceFn;
    protected Item[]    fnArgs;

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MapReduceFn.CombineReduceEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    public void configure(JobConf job)
    {
      super.configure(job);
      reduceFn = compile(job, "reduce", 0);
      fnArgs = new Item[numInputs + 1];
      for (int i = 0; i < numInputs; i++)
      {
        fnArgs[i + 1] = new Item(valArrays[i]);
      }
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
        splitValues(values);
        Item item;
        fnArgs[0] = key;
        Iter iter = reduceFn.iter(context, fnArgs);
        while ((item = iter.next()) != null)
        {
          output.collect(key, item);
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
}
