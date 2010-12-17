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
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.ibm.jaql.io.hadoop.CompositeInputAdapter;
import com.ibm.jaql.io.hadoop.ConfUtil;
import com.ibm.jaql.io.hadoop.Globals;
import com.ibm.jaql.io.hadoop.HadoopAdapter;
import com.ibm.jaql.io.hadoop.HadoopInputAdapter;
import com.ibm.jaql.io.hadoop.HadoopOutputAdapter;
import com.ibm.jaql.io.hadoop.HadoopSerializationMapOutput;
import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.io.hadoop.JsonHolderMapOutputKey;
import com.ibm.jaql.io.hadoop.JsonHolderMapOutputValue;
import com.ibm.jaql.io.hadoop.MapOutputKeyComparator;
import com.ibm.jaql.io.registry.RegistryUtil;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.core.RegisterExceptionHandler;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.ClassLoaderMgr;
import com.ibm.jaql.util.FastPrintBuffer;

/**
 * 
 */
public abstract class MapReduceBaseExpr extends Expr
{
  // Configuration keys
  public final static String    BASE_NAME                  = "com.ibm.jaql.mapred";
  public final static String    STORE_REGISTRY_VAR_NAME    = BASE_NAME + ".sRegistry";
  public final static String    RNG_REGISTRY_VAR_NAME      = BASE_NAME + ".rRegistry";
  public final static String    REGISTRY_VAR_NAM           = BASE_NAME + ".registry";
  public final static String    NUM_INPUTS_NAME            = BASE_NAME + ".numInputs";
  public final static String    SCHEMA_NAME                = BASE_NAME + ".schema";
  public final static String    EXCEPTION_NAME			   = BASE_NAME + ".exceptions";
  
  // Argument keys
  public final static JsonString INPUT_KEY = new JsonString("input");
  public final static JsonString OUTPUT_KEY = new JsonString("output");
  public final static JsonString MAP_KEY = new JsonString("map");
  public final static JsonString SCHEMA_KEY = new JsonString("schema");
  public final static JsonString OPTIONS_KEY = new JsonString("options");
  public final static JsonString CONF_KEY = new JsonString("conf"); // options.conf
  
  
  protected static final Logger LOG = Logger.getLogger(MapReduceFn.class.getName());
 
  protected int                 numInputs;
  protected JobConf             conf;
  protected JsonValue           outArgs;

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

  @Override
  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.READS_EXTERNAL_DATA, true);
    result.put(ExprProperty.HAS_SIDE_EFFECTS, true);
    return result;
  }

  /**
   * This is a tricky question... The expression is evaluated once, but 
   * it returns functions which are evaluated multiple times.
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  
  /**
   * Attempt to find the expression that defines the particular argument to map/reduce.
   * This can return null if expression cannot be located (or cannot exist)
   *  // TODO: Would be much happier with named arguments; switch to them!
   */
  public Expr findArgument(JsonString argName)
  {
    Expr expr = exprs[0];
    if( expr instanceof RecordExpr )
    {
      return ((RecordExpr)expr).findStaticFieldValue(argName);
    }
    else if( expr instanceof ConstExpr )
    {
      JsonRecord rec = (JsonRecord)((ConstExpr)expr).value;
      JsonValue v = rec.get(argName);
      return new ConstExpr(v);
    }
    return null;
  }
  
  /**
   * @param context
   * @return
   * @throws Exception
   */
  protected final JsonRecord baseSetup(Context context) throws Exception
  {
    JsonRecord args = JaqlUtil.enforceNonNull((JsonRecord) exprs[0].eval(context));    
    JsonValue inArgs = args.getRequired(INPUT_KEY);
    outArgs = args.getRequired(OUTPUT_KEY);
    JsonRecord options = (JsonRecord) args.get(OPTIONS_KEY);

    conf = new JobConf(); // TODO: get from context?
    
    // set the default job name
    conf.setJobName("jaql job");
    
    // Set the global options.
    ConfUtil.setConf(conf, (JsonRecord)context.getOptions().get(CONF_KEY));

    File extensions = ClassLoaderMgr.getExtensionJar();
    if (extensions != null)
    {
      conf.setJar(extensions.getAbsolutePath());
    }
    else
    {
      conf.setJarByClass(MapReduceFn.class);
    }

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
    conf.setInt(NUM_INPUTS_NAME, numInputs);
    HadoopInputAdapter inAdapter = (HadoopInputAdapter) JaqlUtil
        .getAdapterStore().input.getAdapter(inArgs);
    inAdapter.setParallel(conf);
    //ConfiguratorUtil.writeToConf(inAdapter, conf, inArgs);

    //
    // Setup the output
    //
//    MemoryJRecord outArgRec = (MemoryJRecord) outArgs.getNonNull();
    HadoopOutputAdapter outAdapter = (HadoopOutputAdapter) JaqlUtil
        .getAdapterStore().output.getAdapter(outArgs);
    outAdapter.setParallel(conf);
//    ConfiguratorUtil.writeToConf(outAdapter, conf, outArgRec);

    // Passs any overrides to the conf from options.conf
    if (options != null)
    {
      JsonRecord confOpts = (JsonRecord)options.get(CONF_KEY);
      ConfUtil.setConf(conf, confOpts);
    }

    // write out various static registries
    RegistryUtil.writeConf(conf, HadoopAdapter.storeRegistryVarName, JaqlUtil
        .getAdapterStore());
    RegistryUtil.writeConf(conf, RNG_REGISTRY_VAR_NAME, JaqlUtil.getRNGStore());
    RegisterExceptionHandler.writeConf(EXCEPTION_NAME, conf);

    return args;
  }

  /** Registers Jaql's serializers. Must be called by subclasses. */
  protected final void setupSerialization(boolean hasReduce)
  {
    // set the intermediate file format, if necessary
    if (hasReduce)
    {
      HadoopSerializationMapOutput.register(conf);
      conf.setMapOutputKeyClass(JsonHolderMapOutputKey.class);
      conf.setMapOutputValueClass(JsonHolderMapOutputValue.class);
      conf.setOutputKeyComparatorClass(MapOutputKeyComparator.class);
    }
  }
  
  /**
   * @param fnName
   * @param numArgs
   * @param fn
   * @param inId
   */
  protected final void prepareFunction(String fnName, int numArgs, Function fn, int inId)
  {
    // TODO: pass functions (and their captures!) as strings through the job conf or a temp file?
    FastPrintBuffer out = new FastPrintBuffer();

    if (fn.getParameters().numParameters() < numArgs || fn.getParameters().numRequiredParameters() > numArgs)
    {
      throw new RuntimeException(fnName + " function must be callable with "
          + numArgs + " positional argument(s)");
    }
    try
    { 	
      JsonUtil.print(out, fn, true);
    } catch (IOException e)
    {
      throw new UndeclaredThrowableException(e);
    }
    String s = out.toString();
    conf.set(BASE_NAME + "." + fnName + "." + inId, s);
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

      numInputs = job.getInt(NUM_INPUTS_NAME, 0);
      assert numInputs > 0;

      runningReduce = (job.get(BASE_NAME + ".reduce.0") != null)
          || (job.get(BASE_NAME + ".final.0") != null);

      // setup global variables
      Globals.setJobConf(job);
      try
      {
        //AdapterManager.readRegistryFromConf(storeRegistryVarName, job);
        RegistryUtil.readConf(job, HadoopAdapter.storeRegistryVarName, JaqlUtil
            .getAdapterStore());
        RegistryUtil.readConf(job, RNG_REGISTRY_VAR_NAME, JaqlUtil.getRNGStore());
        RegisterExceptionHandler.readConf(EXCEPTION_NAME, job);
        //FunctionStore.readRegistryFromConf(funcRegistryVarName, job);
        //RNGStore.readFromConf(rngRegistryVarName, job);
      }
      catch (Exception e)
      {
        throw new UndeclaredThrowableException(e);
      }
    }

    public static Function compile(Context context, String exprText)
    {
      try
      {
        // System.err.println("compiling: "+exprText);
        JaqlLexer lexer = new JaqlLexer(new StringReader(exprText));
        lexer.setTokenObjectClass(com.ibm.jaql.lang.parser.JaqlToken.class.getName());
        JaqlParser parser = new JaqlParser(lexer);
        Expr expr = parser.parse();
        Function fn = JaqlUtil.enforceNonNull((Function) expr.eval(context));
        return fn;
      }
      catch (Exception ex)
      {
        throw new UndeclaredThrowableException(ex);
      }
    }

    public static Function compile(JobConf job, String propName, Context context)
    {
      String exprText = job.get(propName);
      // System.err.println("compiling: "+exprText);
      if (exprText == null)
      {
        throw new RuntimeException("function not found in job conf: " + propName);
      }
      return compile(context, exprText);
    }

    /**
     * @param job
     * @param fnName
     * @param inId
     * @return
     */
    public Function compile(JobConf job, String fnName, int inId)
    {
      String fullName = BASE_NAME + "." + fnName + "." + inId;
      return compile(job, fullName, context);
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
  // produces JsonHolderMapOutputKey / JsonHolderMapOutputValue if there is a reduce phase
  // otherwise produces JsonHolder / JsonHolder
  public static class MapEval extends RemoteEval
      implements MapRunnable<JsonHolder, JsonHolder, JsonHolder, JsonHolder>
  {
    int         inputId     = 0;
    Function   mapFn;
    boolean     makePair    = false;
    BufferedJsonArray outPair;
    JsonHolder outKey = null; 
    JsonHolder outValue = null;

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr.RemoteEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    @Override
    public void configure(JobConf job)
    {
      super.configure(job);

      outKey = (JsonHolder)ReflectionUtils.newInstance(job.getMapOutputKeyClass(), job);
      outValue = (JsonHolder)ReflectionUtils.newInstance(job.getMapOutputValueClass(), job);

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
        mapFn.setArguments(new RecordReaderValueIter(input));
        JsonIterator iter = mapFn.iter(context);
        for (JsonValue v : iter)
        {
          JsonArray inValue = (JsonArray)v;
          assert inValue.count() == 2;
            
          outKey.value = inValue.get(0);
          if (makePair)
          {
            outPair.set(1, inValue.get(1));
            outValue.value = outPair;
          }
          else 
          {
            outValue.value = inValue.get(1);
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
      finally
      {
        this.close();
      }
    }
  }
}
