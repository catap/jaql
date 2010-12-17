/*
 * Copyright (C) IBM Corp. 2009.
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

import java.io.EOFException;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.io.hadoop.ConfUtil;
import com.ibm.jaql.io.hadoop.HadoopOutputAdapter;
import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.io.hadoop.JsonHolderDefault;
import com.ibm.jaql.io.hadoop.SelectSplitInputFormat;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * Run a function *sequentially* but piecemeal over an input array.
 * 
 * chainedMap( 
 * { input: fd,
 *   output: fd,  // TODO: this could be eliminated, but required now and gets state
 *   init: state,
 *   map: fn(part,state) -> state, // part is array of input items
 *   schema?: state schema
 *  })
 * -> state
 */
public class ChainedMapFn extends MapReduceBaseExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("chainedMap", ChainedMapFn.class);
    }
  }
  
  public ChainedMapFn(Expr[] exprs)
  {
    super(exprs);
  }

  public ChainedMapFn(Expr argRec)
  {
    super(argRec);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  protected JsonValue evalRaw(final Context context) throws Exception
  {
    JsonRecord args = baseSetup(context);

    JsonValue state = args.getRequired(new JsonString("init"));
    Function mapFn = (Function)args.getRequired(new JsonString("map"));
    JsonValue schema = args.get(new JsonString("schema"));

    JaqlUtil.enforceNonNull(mapFn);
    
    conf.setNumReduceTasks(0);
    conf.setMapRunnerClass(MapEval.class);

    // setup serialization
    setupSerialization(false);
    if (schema != null)
    {
      conf.set(SCHEMA_NAME, schema.toString());
    }
    
    prepareFunction("map", 2, mapFn, 0);

    InputSplit[] splits = conf.getInputFormat().getSplits(conf, conf.getNumMapTasks());
    
    // Override the input format to select one partition
    int targetSplits = conf.getNumMapTasks();
    String oldFormat = conf.get("mapred.input.format.class");
    conf.set(SelectSplitInputFormat.INPUT_FORMAT, oldFormat);
    // It would be nice to know how many splits we are generating to avoid 
    // using an exception to quit...
    // int numSplits = oldFormat.getSplits(conf, ??);
    // This parameter is avoided in the new API
    conf.setInputFormat(SelectSplitInputFormat.class);
    conf.setNumMapTasks(1);

    DataOutputBuffer buffer = new DataOutputBuffer();
    for( int i = 0 ; i < splits.length ; i++ )
    {
      // TODO: we should move the model around using hdfs files instead of serializing
      conf.setClass(SelectSplitInputFormat.SPLIT_CLASS, splits[i].getClass(), InputSplit.class);
      conf.set(SelectSplitInputFormat.STATE, state.toString());
      buffer.reset();
      splits[i].write(buffer);
      ConfUtil.writeBinary(conf, SelectSplitInputFormat.SPLIT, buffer.getData(), 0, buffer.getLength());
      conf.setJobName("chainedMap "+(i+1)+"/"+splits.length);
      
      // This causes the output file to be deleted.
      HadoopOutputAdapter outAdapter = (HadoopOutputAdapter) 
         JaqlUtil.getAdapterStore().output.getAdapter(outArgs);
      outAdapter.setParallel(conf);

      try
      {
        JobClient.runJob(conf);
      }
      catch( EOFException ex )
      {
        // Thrown when we've processed all of the splits
        break;
      }

      // Read the new state
      final InputAdapter adapter = (InputAdapter) JaqlUtil.getAdapterStore().input.getAdapter(outArgs);
      adapter.open();
      ClosableJsonIterator reader = adapter.iter();
      state = null;
      if( reader.moveNext() )
      {
        state = reader.current();
      }
      reader.close();
    }
    
    return state;
  }

  /**
   * Used for both map and init functions
   */
  public static class MapEval extends RemoteEval
      implements MapRunnable<JsonHolder, JsonHolder, JsonHolder, JsonHolder>
  {
    protected Function mapFn;
    protected JsonValue oldState;

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
      String stateString = job.get(SelectSplitInputFormat.STATE);
      try
      {
        oldState = new JsonParser(new StringReader(stateString)).JsonVal();
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
        mapFn.setArguments(new RecordReaderValueIter(input), oldState);
        JsonValue newState = mapFn.eval(context);
        output.collect(new JsonHolderDefault(), new JsonHolderDefault(newState));
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
}



