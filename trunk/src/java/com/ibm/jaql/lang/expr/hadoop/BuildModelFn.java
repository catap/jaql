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
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.io.hadoop.HadoopOutputAdapter;
import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * Build a data mining model in parallel.
 * 
 * buildModel( 
 * { input: fd,
 *   output: fd,  // TODO: this could be eliminated, but required now and gets model
 *   init: fn() -> model,
 *   partial: fn($part,$model) -> pmodel, // $part is array of input items
 *   combine: fn($pmodels,$model) -> model, // $pmodels is array of partial models
 *   done: fn($oldModel, $newModel) -> bool
 *  })
 * -> model
 */
public class BuildModelFn extends MapReduceBaseExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("buildModel", BuildModelFn.class);
    }
  }
  
  public final static String MODEL_NAME = BASE_NAME + ".model";

  public BuildModelFn(Expr[] exprs)
  {
    super(exprs);
  }

  public BuildModelFn(Expr argRec)
  {
    super(argRec);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(final Context context) throws Exception
  {
    JsonRecord args = baseSetup(context);

    JsonValue model = args.getRequired(new JsonString("initial"));
    Function partialFn = (Function)args.getRequired(new JsonString("partial"));
    Function combineFn = (Function)args.getRequired(new JsonString("combine"));
    Function doneFn = (Function)args.getRequired(new JsonString("done"));
    JsonNumber jmaxIterations = (JsonNumber)args.getRequired(new JsonString("maxIterations"));

    JaqlUtil.enforceNonNull(partialFn);
    JaqlUtil.enforceNonNull(combineFn);
    JaqlUtil.enforceNonNull(doneFn);
    
    long maxIterations = jmaxIterations == null ? 10 : jmaxIterations.longValueExact();

    conf.setNumReduceTasks(0);
    conf.setMapRunnerClass(PartialEval.class);

    // setup serialization
    setupSerialization(false);
    JsonValue schema = args.get(new JsonString("schema"));
    if (schema != null) {
      conf.set(SCHEMA_NAME, schema.toString());
    }
    
    prepareFunction("partial", 2, partialFn, 0);
    
    JsonValue oldModel;
    long iteration = 0;
    JsonBool converged = JsonBool.FALSE;
    do
    {
      // TODO: we should move the model around using hdfs files instead of serializing
      oldModel = model;
      
      iteration++;
      if( iteration > maxIterations )
      {
        break;
      }
      
      conf.set(MODEL_NAME, oldModel.toString());

      // This causes the output file to be deleted.
      HadoopOutputAdapter outAdapter = (HadoopOutputAdapter) 
         JaqlUtil.getAdapterStore().output.getAdapter(outArgs);
      outAdapter.setParallel(conf);

      JobClient.runJob(conf);
     
      final InputAdapter adapter = (InputAdapter) JaqlUtil.getAdapterStore().input.getAdapter(outArgs);
      adapter.open();
      ClosableJsonIterator reader = adapter.iter();
      combineFn.setArguments(reader, oldModel);
      model = combineFn.eval(context);
      reader.close();
      
      doneFn.setArguments(oldModel, model);
      converged = (JsonBool)doneFn.eval(context);
    }
    while( ! JaqlUtil.ebv( converged ));
    
    BufferedJsonRecord result = new BufferedJsonRecord();
    result.add(new JsonString("converged"), converged);
    result.add(new JsonString("iterations"), new JsonLong(iteration));
    result.add(new JsonString("model"), model);
    if( ! JaqlUtil.ebv( converged ) )
    {
      result.add(new JsonString("oldModel"), oldModel);
    }
    
    //    OutputAdapter adapter = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(outArgs);
    //    adapter.open();
    //    ClosableJsonWriter writer = adapter.getWriter();
    //    writer.write(model);
    //    adapter.close();
    
    return result;
  }

  /**
   * Used for both map and init functions
   */
  public static class PartialEval extends RemoteEval
      implements MapRunnable<JsonHolder, JsonHolder, JsonHolder, JsonHolder>
  {
    protected Function partialFn;
    protected JsonValue oldModel;

    /*
     * (non-Javadoc)
     * 
     * @see com.ibm.jaql.lang.expr.hadoop.MapReduceBaseExpr.RemoteEval#configure(org.apache.hadoop.mapred.JobConf)
     */
    @Override
    public void configure(JobConf job)
    {
      super.configure(job);
      partialFn = compile(job, "partial", 0);
      String modelJson = job.get(MODEL_NAME);
      try
      {
        oldModel = new JsonParser(new StringReader(modelJson)).JsonVal();
      }
      catch (ParseException e)
      {
        throw new UndeclaredThrowableException(e);
      }
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
      try
      {
        partialFn.setArguments(new RecordReaderValueIter(input), oldModel);
        JsonValue newModel = partialFn.eval(context);
        output.collect(new JsonHolder(), new JsonHolder(newModel));
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
