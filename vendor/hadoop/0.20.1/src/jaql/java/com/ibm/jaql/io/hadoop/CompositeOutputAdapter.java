/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.io.hadoop;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 *  
 */
public class CompositeOutputAdapter implements HadoopOutputAdapter
{
  protected final static String SUBCONF_NAME = "com.ibm.jaql.CompositeOutputAdapter.subconf.";
  public final static JsonString DESCRIPTORS = new JsonString("descriptors");
  
  protected JsonRecord args;
  protected JsonArray descriptors;
  // TODO: Can we make this just OutputFormat instead of HadoopOutputAdapter
  protected HadoopOutputAdapter[] outputs;
  protected JobConf[] subconfs;
  
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.io.Initializable#init(com.ibm.jaql.json.type.JsonValue)
   */
  @Override
  public void init(JsonValue options) throws Exception
  {
    // Initialize the outputs from our options
    args = (JsonRecord)options;
    descriptors = (JsonArray)args.get(DESCRIPTORS);
    int numOutputs = (int) descriptors.count();
    outputs = new HadoopOutputAdapter[numOutputs];
    for( int i = 0 ; i < numOutputs ; i++ )
    {
      JsonValue fd = descriptors.get(i);
      outputs[i] = (HadoopOutputAdapter) AdapterStore.getStore().output.getAdapter(fd);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.JobConfigurable#configure(org.apache.hadoop.mapred.JobConf)
   */
  @Override
  public void configure(JobConf conf)
  {
    // TODO: is this needed? How should it get done once?
    //    Globals.setJobConf(conf);
    //    try
    //    {
    //      RegistryUtil.readConf(conf, HadoopAdapter.storeRegistryVarName,
    //          AdapterStore.getStore());
    //    }
    //    catch (Exception e)
    //    {
    //      throw new RuntimeException(e);
    //    }
    
    // read in the adapter array from conf and initialize it
    try
    {
      descriptors = ConfUtil.readConfArray(conf, ConfSetter.CONFOUTOPTIONS_NAME);
      int numOutputs = (int) descriptors.count();
      outputs = new HadoopOutputAdapter[numOutputs];
      subconfs = new JobConf[numOutputs];
      for( int i = 0 ; i < outputs.length ; i++ )
      {
        JsonValue fd = descriptors.get(i);
        outputs[i] = (HadoopOutputAdapter) AdapterStore.getStore().output.getAdapter(fd);
        subconfs[i] = restoreConf(conf, conf.get(SUBCONF_NAME + i));
        outputs[i].configure(subconfs[i]);
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Parse the JSON record in confText.
   * Added each field and its value to a new JobConf.
   *  
   * @param conf
   * @param confText
   * @return
   */
  private static JobConf restoreConf(JobConf parent, String confText) // TODO: move to general library
  {
    try
    {
      JsonParser parser = new JsonParser(new StringReader(confText));
      JsonRecord jrec = (JsonRecord)parser.JsonVal();
      JobConf conf = new JobConf(parent);
      for( Map.Entry<JsonString, JsonValue> entry: jrec )
      {
        JsonValue val = entry.getValue();
        conf.set(entry.getKey().toString(), val == null ? null : val.toString());
      }
      return conf;
    }
    catch(ParseException pe)
    {
      throw new UndeclaredThrowableException(pe); // IOException(pe);
    }
  }

  /**
   * For each entry in conf, if its value differs from the value in parent,
   * add it to the result JSON record.
   *  
   * @param conf
   * @param confText
   * @return
   */
  private static JsonRecord saveConf(JobConf parent, JobConf conf)  // TODO: move to general library
  {
    BufferedJsonRecord jrec = new BufferedJsonRecord();
    // Add all entries in conf that are not in parent
    for( Map.Entry<String, String> entry: conf )
    {
      String name = entry.getKey();
      String value = entry.getValue();
      String value2 = parent.getRaw(name);
      if( value != value2 && ! value.equals(value2) )
      {
        jrec.add(new JsonString(name), new JsonString(value));
      }
    }
//    // Remove all entries in parent that are not in conf
//    for( Map.Entry<String, String> entry: parent )
//    {
//      String name = entry.getKey();
//      if( conf.getRaw(name) == null )
//      {
//        jrec.add(new JsonString(name), null);
//      }
//    }
    return jrec;
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.io.Adapter#open()
   */
  @Override
  public void open() throws Exception
  {
    for( HadoopOutputAdapter out: outputs )
    {
      out.open();
    }
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.io.Adapter#close()
   */
  @Override
  public void close() throws Exception
  {
    // TODO: we should make an effort to close all outputs, 
    //       even if an earlier one throws an exception
    int n = outputs.length;
    for(int i = 0 ; i < n ; i++)
    {
      if( outputs[i] != null )
      {
        outputs[i].close();
        outputs[i] = null;
      }
    }
  }

  private void set(JobConf conf) throws Exception
  {
    // TODO: Should this call be passed to outputs?
    conf.setOutputFormat(this.getClass());
    conf.setOutputCommitter(CompositeCommitter.class);
    conf.setOutputKeyClass(JsonHolderDefault.class);
    conf.setOutputValueClass(JsonHolderDefault.class);
    HadoopSerializationDefault.register(conf);
    
    // We do not support speculative execution at this time.
    conf.setSpeculativeExecution(false);

    // write out the input adapter args array
    ConfUtil.writeConfArray(conf, ConfSetter.CONFOUTOPTIONS_NAME, descriptors);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.io.hadoop.ConfSetter#setParallel(org.apache.hadoop.mapred.JobConf)
   */
  @Override
  public void setParallel(JobConf conf) throws Exception
  {
    set(conf);

    subconfs = new JobConf[outputs.length];
    for( int i = 0 ; i < outputs.length ; i++ )
    {
      subconfs[i] = new JobConf(conf);
      // We need to set the committer because many class rely on this default (instead of our committer)
      subconfs[i].setOutputCommitter(FileOutputCommitter.class);
      outputs[i].setParallel(subconfs[i]);
      JsonRecord confText = saveConf(conf, subconfs[i]);
      conf.set(SUBCONF_NAME + i, confText.toString());
    }
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.io.hadoop.ConfSetter#setSequential(org.apache.hadoop.mapred.JobConf)
   */
  @Override
  public void setSequential(JobConf conf) throws Exception
  {
    // TODO: Should this call be passed to outputs?
    set(conf);
    subconfs = new JobConf[outputs.length];
    for( int i = 0 ; i < outputs.length ; i++ )
    {
      subconfs[i] = new JobConf(conf);
      outputs[i].setSequential(subconfs[i]);
    }
  }


  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.OutputFormat#checkOutputSpecs(org.apache.hadoop.fs.FileSystem, org.apache.hadoop.mapred.JobConf)
   */
  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf conf)
      throws IOException
  {
    for(int i = 0 ; i < outputs.length ; i++)
    {
      outputs[i].checkOutputSpecs(ignored, subconfs[i]);
      
      // HACK: Hadoop 0.18 has hacks that specialize FileOutputFormat handling. In particular,
      // the temporary directory is created by the Task or LocalJobRunner; they also promote
      // the temporary files to the parent upon completion.  We create the temporary file here,
      // if it doesn't already exist.  On
      Path outputPath = FileOutputFormat.getOutputPath(subconfs[i]);
      if (outputPath != null) 
      {
        final String TEMP_DIR_NAME = "_temporary"; // MRConstants isn't public...
        Path jobTmpDir = new Path(outputPath, TEMP_DIR_NAME); // MRConstants.TEMP_DIR_NAME
        FileSystem fs = jobTmpDir.getFileSystem(subconfs[i]);
        if( !fs.exists(jobTmpDir) )
        {
          fs.mkdirs(jobTmpDir);
        }
      }
    }
  }
  
  // TODO: need to move to the new API!
  //  @Override
  //  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
  //    throws IOException, InterruptedException
  //  {
  //    return new CompositeCommitter();
  //  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.OutputFormat#getRecordWriter(org.apache.hadoop.fs.FileSystem, org.apache.hadoop.mapred.JobConf, java.lang.String, org.apache.hadoop.util.Progressable)
   */
  @Override
  public RecordWriter<JsonHolder,JsonHolder> getRecordWriter(
      FileSystem ignored, 
      JobConf conf,
      String name, 
      Progressable progress) throws IOException
  {
    final RecordWriter<JsonHolder, JsonHolder>[] writers = new RecordWriter[outputs.length];
    final JsonHolder[] outKey = new JsonHolder[outputs.length];
    final JsonHolder[] outValue = new JsonHolder[outputs.length];
    
//    final Path[] taskOutputPaths = new Path[outputs.length]; // HACK: Hadoop 0.18
    for(int i = 0 ; i < outputs.length ; i++)
    {
//      Path outputPath = FileOutputFormat.getOutputPath(subconfs[i]);
//      if( outputPath != null )
//      {
//        final String TEMP_DIR_NAME = "_temporary"; // MRConstants isn't public...
//        taskOutputPaths[i] = new Path(outputPath,
//            (TEMP_DIR_NAME + Path.SEPARATOR + "_" + name));
//      }      
      writers[i] = outputs[i].getRecordWriter(ignored, subconfs[i], name, progress);
      outKey[i] = (JsonHolder)ReflectionUtils.newInstance(subconfs[i].getOutputKeyClass(), subconfs[i]);
      outValue[i] = (JsonHolder)ReflectionUtils.newInstance(subconfs[i].getOutputValueClass(), subconfs[i]);
    }
    
    return new RecordWriter<JsonHolder, JsonHolder>()
    {
      @Override
      public void write(JsonHolder key, JsonHolder value) throws IOException
      {
        JsonArray pair = (JsonArray)value.value;
        if( pair != null )
        {
          try
          {
            JsonNumber n = (JsonNumber)pair.get(0);
            int i = (int)n.longValueExact();
            outKey[i].value = key.value;
            outValue[i].value =  pair.get(1);
            writers[i].write(outKey[i], outValue[i]);
          }
          catch( Exception e )
          {
            throw new UndeclaredThrowableException(e);
          }
        }
      }
      
      @Override
      public void close(Reporter reporter) throws IOException
      {
        for(int i = 0 ; i < writers.length ; i++)
        {
          writers[i].close(reporter);
          
          // HACK: Hadoop 0.18
//          Path taskOutput = taskOutputPaths[i];
//          if(taskOutput != null)
//          {
//            FileSystem fs = taskOutput.getFileSystem(subconfs[i]);
//            if( fs.exists(taskOutput) )
//            {
//              Path jobOutputPath = taskOutput.getParent().getParent();
//
//              // Move the task outputs to their final place
//              // Path finalOutputPath = getFinalPath(jobOutputPath, taskOutput);
//              Path finalOutputPath = new Path(jobOutputPath, taskOutput.getName());
//              if( !fs.rename(taskOutput, finalOutputPath) )
//              {
//                if( !fs.delete(finalOutputPath, true) )
//                {
//                  throw new IOException("Failed to delete earlier output of task");
//                }
//                if( !fs.rename(taskOutput, finalOutputPath) )
//                {
//                  throw new IOException("Failed to save output of task: ");
//                }
//              }
//              // LOG.debug("Moved " + taskOutput + " to " + finalOutputPath);
//
//              // Delete the temporary task-specific output directory
//              if (!fs.delete(taskOutput, true)) {
//                // LOG.info("Failed to delete the temporary output directory of task: " + 
//                //    getTaskID() + " - " + taskOutputPath);
//              }
//              // LOG.info("Saved output of task '" + getTaskID() + "' to " + jobOutputPath);
//            }
//          }
        }
      }
    };
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.io.OutputAdapter#getWriter()
   */
  @Override
  public ClosableJsonWriter getWriter() throws Exception
  {
    final ClosableJsonWriter[] writers = new ClosableJsonWriter[outputs.length];
    for(int i = 0 ; i < outputs.length ; i++)
    {
      writers[i] = outputs[i].getWriter();
    }
    
    return new ClosableJsonWriter()
    {
      @Override
      public void write(JsonValue value) throws IOException
      {
        try
        {
          JsonArray pair = (JsonArray)value;
          JsonNumber n = (JsonNumber)pair.get(0);
          int i = (int)n.longValueExact();
          writers[i].write(pair.get(1));
        }
        catch( Exception e )
        {
          throw new UndeclaredThrowableException(e);
        }
      }

      @Override
      public void close() throws IOException
      {
        for( ClosableJsonWriter w: writers )
        {
          w.close();
        }
      }

    };
  }
  
  protected static class CompositeCommitter extends OutputCommitter
  {
    /** Finds constructors, including private ones. */
    @SuppressWarnings("unchecked")
    public static <T> Constructor<T> getConstructor(Class<T> cls, Class<?>... parameterTypes) 
    {
      for( Constructor<?> cons: cls.getDeclaredConstructors() )
      {
        Class<?>[] p = cons.getParameterTypes();
        if( p.length == parameterTypes.length )
        {
          for(int i = 0 ; i < p.length ; i++)
          {
            if( p[i].isAssignableFrom(parameterTypes[i]) )
            {
              cons.setAccessible(true); 
              return (Constructor<T>)cons;
            }
          }
        }
      }
      throw new RuntimeException("no such constructor on "+cls);
    }
    
    /** A horrible hack to get around mapred package protection.  Eliminate when we move to mapreduce package */
    public static TaskAttemptContext newTaskAttemptContext(JobConf conf, TaskAttemptID taskid)
    {
      try
      {
        Constructor<TaskAttemptContext> cons = 
          getConstructor(TaskAttemptContext.class, JobConf.class, TaskAttemptID.class);
        return cons.newInstance(conf, taskid);
      }
      catch (Exception e)
      {
        throw JaqlUtil.rethrow(e);
      }
    }

    /** A horrible hack to get around mapred package protection.  Eliminate when we move to mapreduce package */
    public static JobContext newJobContext(JobConf conf, org.apache.hadoop.mapreduce.JobID jobid)
    {
      try
      {
        Constructor<JobContext> cons = 
          getConstructor(JobContext.class, JobConf.class, org.apache.hadoop.mapreduce.JobID.class);
        return cons.newInstance(conf, jobid);
      }
      catch (Exception e)
      {
        throw JaqlUtil.rethrow(e);
      }
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException
    {
      // TODO: need something like this if we want to support inheritance, but we need to update to new mapreduce api
      //      CompositeOutputAdapter coa = new (CompositeOutputAdapter)
      //      ReflectionUtils.newInstance(
      //          context.getOutputFormatClass(), 
      //          context.getConfiguration());
      CompositeOutputAdapter coa = new CompositeOutputAdapter();
      coa.configure(context.getJobConf());
      for( int i = 0 ; i < coa.outputs.length ; i++ )
      {
        JobConf subconf = coa.subconfs[i];
        TaskAttemptContext subcontext = newTaskAttemptContext(subconf, context.getTaskAttemptID());
        OutputCommitter committer = subconf.getOutputCommitter();
        committer.setupTask(subcontext);
      }
    }
    
    @Override
    public void abortTask(TaskAttemptContext context) throws IOException
    {
      CompositeOutputAdapter coa = new CompositeOutputAdapter();
      coa.configure(context.getJobConf());
      for( int i = 0 ; i < coa.outputs.length ; i++ )
      {
        JobConf subconf = coa.subconfs[i];
        TaskAttemptContext subcontext = newTaskAttemptContext(subconf, context.getTaskAttemptID());
        OutputCommitter committer = subconf.getOutputCommitter();
        committer.abortTask(subcontext);
      }
    }

    @Override
    public void cleanupJob(JobContext context) throws IOException
    {
      CompositeOutputAdapter coa = new CompositeOutputAdapter();
      coa.configure(context.getJobConf());
      for( int i = 0 ; i < coa.outputs.length ; i++ )
      {
        JobConf subconf = coa.subconfs[i];
        JobContext subcontext = newJobContext(subconf, context.getJobID());
        OutputCommitter committer = subconf.getOutputCommitter();
        committer.cleanupJob(subcontext);
      }
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException
    {
      CompositeOutputAdapter coa = new CompositeOutputAdapter();
      coa.configure(context.getJobConf());
      for( int i = 0 ; i < coa.outputs.length ; i++ )
      {
        JobConf subconf = coa.subconfs[i];
        TaskAttemptContext subcontext = newTaskAttemptContext(subconf, context.getTaskAttemptID());
        OutputCommitter committer = subconf.getOutputCommitter();
        committer.commitTask(subcontext);
      }
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException
    {
      CompositeOutputAdapter coa = new CompositeOutputAdapter();
      coa.configure(context.getJobConf());
      for( int i = 0 ; i < coa.outputs.length ; i++ )
      {
        JobConf subconf = coa.subconfs[i];
        TaskAttemptContext subcontext = newTaskAttemptContext(subconf, context.getTaskAttemptID());
        OutputCommitter committer = subconf.getOutputCommitter();
        if( committer.needsTaskCommit(subcontext) )
        {
          return true;
        }
      }
      return false;
    }

    @Override
    public void setupJob(JobContext context) throws IOException
    {
      CompositeOutputAdapter coa = new CompositeOutputAdapter();
      coa.configure(context.getJobConf());
      for( int i = 0 ; i < coa.outputs.length ; i++ )
      {
        JobConf subconf = coa.subconfs[i];
        JobContext subcontext = newJobContext(subconf, context.getJobID());
        OutputCommitter committer = subconf.getOutputCommitter();
        committer.setupJob(subcontext);
      }
    }

  }

  /**
   * Make the descriptor to write or read a list of descriptors
   * 
   * { inoptions:  { adapter: 'com.ibm.jaql.io.hadoop.CompositeInputAdapter' },
   *   outoptions: { adapter: 'com.ibm.jaql.io.hadoop.CompositeOutputAdapter' },
   *   descriptors: $descriptors
   * }
   */
  public static Expr makeDescriptor(Expr[] descriptors)
  {
    BufferedJsonRecord inoptions = new BufferedJsonRecord(1);
    inoptions.add(ADAPTER_NAME, new JsonString(CompositeInputAdapter.class.getName())); // TODO: make consts
    BufferedJsonRecord outoptions = new BufferedJsonRecord(1);
    outoptions.add(ADAPTER_NAME, new JsonString(CompositeOutputAdapter.class.getName()));
    RecordExpr desc = new RecordExpr(new Expr[]{
        new NameValueBinding(INOPTIONS_NAME, new ConstExpr(inoptions)),
        new NameValueBinding(OUTOPTIONS_NAME, new ConstExpr(outoptions)),
        new NameValueBinding(DESCRIPTORS, new ArrayExpr(descriptors))
    });
    return desc;
  }
}
