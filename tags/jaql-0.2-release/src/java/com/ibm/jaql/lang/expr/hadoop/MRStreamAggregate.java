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
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

// TODO: This is a work in progress.  It is not useful yet!!

/*
 * mrAggregate({ input: [ {A}, {B} ], output: {C}, using: [ reduce $i in ??$A??
 * 
 * mrAggregate({ input: [ bind($i, {A}), bind($j, {B}) ], output: {C}, using: [ [
 * sumPA([$i*$i]), []
 * 
 */
@JaqlFn(fnName = "mrStreamAggregate", minArgs = 1, maxArgs = 1)
public class MRStreamAggregate extends MapReduceBaseExpr
{

  /**
   * mrAggregate( record args ) { input: [ descriptors ], group: [ keys ] using: [
   * reduces ], output: descriptor }
   * 
   * @param exprs
   */
  public MRStreamAggregate(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param args
   */
  public MRStreamAggregate(Expr args)
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
    Item init = args.getRequired("init");
    Item combine = args.getRequired("combine");
    Item finl = args.getRequired("final");

    // use default: conf.setNumMapTasks(10); // TODO: need a way to specify options
    // use default: conf.setNumReduceTasks(2); // TODO: get from options
    conf.setMapRunnerClass(MapEval.class);
    conf.setCombinerClass(AggCombineEval.class);
    conf.setReducerClass(AggFinalEval.class);

    JFunction finalFn = (JFunction) finl.getNonNull();
    prepareFunction("final", numInputs + 1, finalFn, 0);

    if (numInputs == 1)
    {
      JFunction initFn = (JFunction) init.getNonNull();
      prepareFunction("map", 1, initFn, 0); // called map to share the same MapEval class
      JFunction combineFn = (JFunction) combine.getNonNull();
      prepareFunction("combine", 3, combineFn, 0);
    }
    else
    {
      JArray initArray = (JArray) init.getNonNull();
      Iter iter = initArray.iter();
      for (int i = 0; i < numInputs; i++)
      {
        JFunction initFn = (JFunction) iter.next().getNonNull();
        prepareFunction("map", 1, initFn, i); // called map to share the same MapEval class
      }
      JArray combineArray = (JArray) combine.getNonNull();
      iter = combineArray.iter();
      for (int i = 0; i < numInputs; i++)
      {
        JFunction combineFn = (JFunction) iter.next().getNonNull();
        prepareFunction("combine", 3, combineFn, i);
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
  public static class AggCombineEval extends RemoteEval
      implements
        Reducer<Item, Item, Item, Item>
  {
    protected int         inputId;
    protected Item[]      combineArgs = new Item[3]; // key, val1, val2
    protected JFunction[] usingFns;
    protected Item[]      agg;
    protected JLong       outId;
    protected FixedJArray outPair;
    protected Item        outItem;
    private boolean[]     seen;

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr.RemoteEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    public void configure(JobConf job)
    {
      super.configure(job);
      usingFns = new JFunction[numInputs];
      agg = new Item[numInputs];
      seen = new boolean[numInputs];
      for (int i = 0; i < numInputs; i++)
      {
        agg[i] = new Item();
        usingFns[i] = compile(job, "combine", inputId);
        // FIXME: ideally we could know which input was used to when reading map/combine output files without encoding it on every record
      }
      if (numInputs > 1)
      {
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
        combineArgs[0] = key;

        for (int i = 0; i < numInputs; i++)
        {
          agg[i].set(null);
          seen[i] = false;
        }

        while (values.hasNext())
        {
          Item inItem = values.next();
          int i = 0;
          if (numInputs > 1)
          {
            FixedJArray valPair = (FixedJArray) inItem.get();
            JLong id = (JLong) valPair.get(0).getNonNull();
            i = (int) id.value;
            inItem = valPair.get(1);
          }
          seen[i] = true;
          if (!inItem.isNull())
          {
            Item combined;
            if (agg[i].isNull())
            {
              combined = inItem;
            }
            else
            {
              combineArgs[1] = agg[i];
              combineArgs[2] = inItem;
              combined = usingFns[i].eval(context, combineArgs);
              if (combined.isNull())
              {
                throw new RuntimeException("combiners cannot return null");
              }
            }
            agg[i].copy(combined);
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

    /**
     * @param key
     * @param output
     * @throws Exception
     */
    protected void processAggs(Item key, OutputCollector<Item, Item> output)
        throws Exception
    {
      if (numInputs == 1)
      {
        assert seen[0];
        output.collect(key, agg[0]);
      }
      else
      {
        for (int i = 0; i < numInputs; i++)
        {
          if (seen[i])
          {
            outId.value = i;
            outPair.set(1, agg[i]);
            output.collect(key, outItem);
          }
        }
      }
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
     * @see com.ibm.jaql.lang.expr.hadoop.MRStreamAggregate.AggCombineEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    public void configure(JobConf job)
    {
      super.configure(job);
      finalFn = compile(job, "final", 0);
      finalArgs = new Item[numInputs + 1];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MRStreamAggregate.AggCombineEval#processAggs(com.ibm.jaql.json.type.Item,
     *      org.apache.hadoop.mapred.OutputCollector)
     */
    @Override
    protected void processAggs(Item key, OutputCollector<Item, Item> output)
        throws Exception
    {
      finalArgs[0] = key;
      for (int i = 0; i < numInputs; i++)
      {
        finalArgs[i + 1] = agg[i];
      }
      Item item;
      Iter iter = finalFn.iter(context, finalArgs);
      while ((item = iter.next()) != null)
      {
        output.collect(key, item);
      }
    }
  }
}
