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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.ibm.jaql.io.hadoop.CompositeInputAdapter;
import com.ibm.jaql.io.hadoop.Globals;
import com.ibm.jaql.io.hadoop.HadoopAdapter;
import com.ibm.jaql.io.hadoop.HadoopInputAdapter;
import com.ibm.jaql.io.hadoop.HadoopOutputAdapter;
import com.ibm.jaql.io.registry.RegistryUtil;
import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * 
 */
public abstract class MapReduceBaseExpr extends Expr
{
  public final static String    IMP                  = "com.ibm.impliance.MapReduceExpr";
  public final static String    storeRegistryVarName = IMP + ".sRegistry";
  public final static String    funcRegistryVarName  = IMP + ".fRegistry";
  public final static String    rngRegistryVarName   = IMP + ".rRegistry";
  public final static String    registryVarName      = IMP + ".registry";
  public final static String    numInputsName        = IMP + ".numInputs";

  protected static final Logger LOG                  = Logger
                                                         .getLogger(MapReduceFn.class
                                                             .getName());

  protected int                 numInputs;
  protected JobConf             conf;
  protected Item                outArgs;

  /**
   * mapReduce( record args )
   * 
   * @param exprs
   */
  public MapReduceBaseExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param args
   */
  public MapReduceBaseExpr(Expr args)
  {
    this(new Expr[]{args});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isConst()
   */
  @Override
  public boolean isConst()
  {
    return false;
  }

  /**
   * @param context
   * @return
   * @throws Exception
   */
  protected final JRecord baseSetup(Context context) throws Exception
  {
    JRecord args = (JRecord) exprs[0].eval(context).getNonNull();
    Item inArgs = args.getRequired("input");
    outArgs = args.getRequired("output");
    JRecord options = (JRecord) args.getNull("options");

    conf = new JobConf();
    File extensions = ClassLoaderMgr.getExtensionJar();
    if (extensions != null)
    {
      conf.setJar(extensions.getAbsolutePath());
    }
    else
    {
      conf.setJarByClass(MapReduceFn.class);
    }

    // setup the job name
    String jobName = "jaql job";
    if (options != null)
    {
      Item nameItem = options.getValue("jobname");
      if (nameItem != null)
      {
        jobName = nameItem.toString();
      }
    }
    conf.setJobName(jobName);

    //
    // Force local execution if requested.
    //
    if( "local".equals(System.getProperty("jaql.mapred.mode")) ) // TODO: pick this up from the context instead
    {
      conf.set("mapred.job.tracker", "local");
    }
    
    //
    // Setup the input
    //
    if (inArgs.getNonNull() instanceof JArray)
    {
      JArray inArray = (JArray) inArgs.get();
      numInputs = (int) inArray.count();
      if (numInputs < 1)
      {
        throw new RuntimeException("at least one input expected");
      }
    }
    else
    {
      numInputs = 1;
    }
    conf.setInt(numInputsName, numInputs);
    HadoopInputAdapter inAdapter = (HadoopInputAdapter) JaqlUtil
        .getAdapterStore().input.getAdapter(inArgs);
    inAdapter.setParallel(conf);
    //ConfiguratorUtil.writeToConf(inAdapter, conf, inArgs);

    //
    // Setup the output
    //
    MemoryJRecord outArgRec = (MemoryJRecord) outArgs.getNonNull();
    HadoopOutputAdapter outAdapter = (HadoopOutputAdapter) JaqlUtil
        .getAdapterStore().output.getAdapter(outArgs);
    outAdapter.setParallel(conf);
    //ConfiguratorUtil.writeToConf(outAdapter, conf, outArgRec);

    // write out various static registries
    RegistryUtil.writeConf(conf, HadoopAdapter.storeRegistryVarName, JaqlUtil
        .getAdapterStore());
    RegistryUtil.writeConf(conf, funcRegistryVarName, JaqlUtil
        .getFunctionStore());
    RegistryUtil.writeConf(conf, rngRegistryVarName, JaqlUtil.getRNGStore());

    return args;
  }

  /**
   * @param fnName
   * @param numArgs
   * @param fn
   * @param inId
   */
  protected final void prepareFunction(String fnName, int numArgs,
      JFunction fn, int inId)
  {
    // TODO: pass functions (and their captures!) as strings through the job conf or a temp file?
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(outStream);

    if (fn.getNumParameters() != numArgs)
    {
      throw new RuntimeException(fnName + " function must have exactly "
          + numArgs + " argument(s)");
    }
    fn.print(ps);
    ps.flush();
    String s = outStream.toString();
    conf.set(IMP + "." + fnName + "." + inId, s);
  }

  /**
   * 
   */
  public static abstract class RemoteEval
  {
    protected Context context = new Context();
    protected int     numInputs;
    protected boolean runningReduce;

    public void configure(JobConf job)
    {
      // This might be useful for temps
      // String taskId = job.get("mapred.task.id");

      numInputs = job.getInt(numInputsName, 0);
      assert numInputs > 0;

      runningReduce = (job.get(IMP + ".reduce.0") != null)
          || (job.get(IMP + ".final.0") != null);

      // setup global variables
      Globals.setJobConf(job);
      try
      {
        //AdapterManager.readRegistryFromConf(storeRegistryVarName, job);
        RegistryUtil.readConf(job, HadoopAdapter.storeRegistryVarName, JaqlUtil
            .getAdapterStore());
        RegistryUtil.readConf(job, funcRegistryVarName, JaqlUtil
            .getFunctionStore());
        RegistryUtil.readConf(job, rngRegistryVarName, JaqlUtil.getRNGStore());
        //FunctionStore.readRegistryFromConf(funcRegistryVarName, job);
        //RNGStore.readFromConf(rngRegistryVarName, job);
      }
      catch (Exception e)
      {
        throw new UndeclaredThrowableException(e);
      }
    }

    /**
     * @param job
     * @param fnName
     * @param inId
     * @return
     */
    public JFunction compile(JobConf job, String fnName, int inId)
    {
      try
      {
        String fullName = IMP + "." + fnName + "." + inId;
        String exprText = job.get(fullName);
        // System.err.println("compiling: "+exprText);
        if (exprText == null)
        {
          throw new RuntimeException("function not found in job conf: "
              + fullName);
        }
        JaqlLexer lexer = new JaqlLexer(new StringReader(exprText));
        JaqlParser parser = new JaqlParser(lexer);
        Expr expr = parser.parse();
        JFunction fn = (JFunction) expr.eval(context).getNonNull();
        return fn;
      }
      catch (Exception ex)
      {
        throw new UndeclaredThrowableException(ex);
      }
    }

    public void close() throws IOException
    {
      // TODO: might want sub-query indicator
      context.endQuery(); // TODO: need to wrap up parse, eval, cleanup into one class and use everywhere
    }
  }

  /**
   * Used for both map and init functions
   */
  public static class MapEval extends RemoteEval
      implements MapRunnable<Item, Item, Item, Item>
  {
    int         inputId     = 0;
    JFunction   mapFn;
    Item[]      outKeyValue = new Item[2];
    boolean     makePair    = false;
    FixedJArray outPair;
    Item        pairItem;

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr.RemoteEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    @Override
    public void configure(JobConf job)
    {
      super.configure(job);
      if (numInputs > 1)
      {
        inputId = CompositeInputAdapter.readCurrentIndex(job);
        makePair = runningReduce;
        if (makePair)
        {
          // FIXME: ideally we could know which input was used to when reading map/combine output files without encoding it on every record
          outPair = new FixedJArray(2);
          outPair.set(0, new Item(new JLong(inputId)));
          pairItem = new Item(outPair);
        }
      }
      mapFn = compile(job, "map", inputId);
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
      try
      {
        mapFn.param(0).set(new RecordReaderValueIter(input));
        Iter iter = mapFn.iter(context);
        Item item;
        while ((item = iter.next()) != null)
        {
          JArray pair = (JArray) item.get();
          if (pair != null)
          {
            assert pair.count() == 2;
            Item val;
            if (makePair)
            {
              outPair.set(1, pair.nth(1));
              val = pairItem;
            }
            else 
            {
              val = pair.nth(1);
            }
            output.collect(pair.nth(0), val);
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
}
