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
import java.util.Map;

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
import com.ibm.jaql.io.hadoop.HadoopSerialization;
import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.io.registry.RegistryUtil;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.DefaultJsonComparator;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JaqlFunction;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
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
  protected JsonValue              outArgs;

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

  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.READS_EXTERNAL_DATA, true);
    return result;
  }

  /**
   * @param context
   * @return
   * @throws Exception
   */
  protected final JsonRecord baseSetup(Context context) throws Exception
  {
    JsonRecord args = JaqlUtil.enforceNonNull((JsonRecord) exprs[0].eval(context));    
    JsonValue inArgs = args.getRequired(new JsonString("input"));
    outArgs = args.getRequired(new JsonString("output"));
    JsonRecord options = (JsonRecord) args.get(new JsonString("options"));

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
      JsonValue nameValue = options.get(new JsonString("jobname"));
      if (nameValue != null)
      {
        jobName = nameValue.toString();
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
    if (inArgs instanceof JsonArray)
    {
      JsonArray inArray = (JsonArray) inArgs;
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
    // Setup serialization
    
    // TODO: currently assumes usage of  FullSerializer#getDefault()
    HadoopSerialization.register(conf);
    conf.setOutputKeyComparatorClass(DefaultJsonComparator.class);
    
    //
    // Setup the output
    //
//    MemoryJRecord outArgRec = (MemoryJRecord) outArgs.getNonNull();
    HadoopOutputAdapter outAdapter = (HadoopOutputAdapter) JaqlUtil
        .getAdapterStore().output.getAdapter(outArgs);
    outAdapter.setParallel(conf);
//    ConfiguratorUtil.writeToConf(outAdapter, conf, outArgRec);

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
      JaqlFunction fn, int inId)
  {
    // TODO: pass functions (and their captures!) as strings through the job conf or a temp file?
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(outStream);

    if (fn.getNumParameters() != numArgs)
    {
      throw new RuntimeException(fnName + " function must have exactly "
          + numArgs + " argument(s)");
    }
    try
    {
      JsonUtil.print(ps, fn);
    } catch (IOException e)
    {
      throw new UndeclaredThrowableException(e);
    }
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
    public JaqlFunction compile(JobConf job, String fnName, int inId)
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
        JaqlFunction fn = JaqlUtil.enforceNonNull((JaqlFunction) expr.eval(context));
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
      context.reset(); // TODO: need to wrap up parse, eval, cleanup into one class and use everywhere
    }
  }

  /**
   * Used for both map and init functions
   */
  public static class MapEval extends RemoteEval
      implements MapRunnable<JsonHolder, JsonHolder, JsonHolder, JsonHolder>
  {
    int         inputId     = 0;
    JaqlFunction   mapFn;
    boolean     makePair    = false;
    BufferedJsonArray outPair;
    JsonHolder outKey = new JsonHolder();
    JsonHolder outValue = new JsonHolder();

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
          outPair = new BufferedJsonArray(2);
          outPair.set(0, new JsonLong(inputId));
        }
      }
      mapFn = compile(job, "map", inputId);
    }

    @Override
    public void run( RecordReader<JsonHolder, JsonHolder> input,
                     OutputCollector<JsonHolder, JsonHolder> output, 
                     Reporter reporter) 
      throws IOException
    {
      try
      {
        JsonIterator iter = mapFn.iter(context, new RecordReaderValueIter(input));
        for (JsonValue v : iter)
        {
          JsonArray inValue = (JsonArray)v;
          assert inValue.count() == 2;
            
          outKey.value = inValue.nth(0);
          if (makePair)
          {
            outPair.set(1, inValue.nth(1));
            outValue.value = outPair;
          }
          else 
          {
            outValue.value = inValue.nth(1);
          }
          output.collect(outKey, outValue);
        }
      }
      catch (IOException ex)
      {
        throw ex;
      } 
      catch (Exception e)
      {
        throw new UndeclaredThrowableException(e);
      }
    }
  }
}
