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
package com.ibm.jaql.io.hadoop;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ExposeJobContext;
import org.apache.hadoop.mapred.ExposeTaskAttemptContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.io.hadoop.converter.KeyValueExport;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

/**
 * The default class for writing Items from jaql to Hadoop
 */
public class DefaultHadoopOutputAdapter<K,V> implements HadoopOutputAdapter
{
  static final Logger                LOG    = Logger.getLogger(DefaultHadoopOutputAdapter.class.getName());

  protected static final AtomicInteger jobCounter = new AtomicInteger(0);

  protected OutputFormat<?,?>             oFormat;

  protected InitializableConfSetter           configurator;

  protected KeyValueExport<K, V>       converter;

  protected JobConf                  conf;

  protected BufferedJsonRecord            args;                      // original arguments

  protected String                   location;

  protected BufferedJsonRecord            options;                   // options from arguments unioned with
  // defaults in registry

  protected Progressable             reporter;

  protected RecordWriter<JsonHolder, JsonHolder> writer = null;

  JobContext sequentialJob;  // job context for sequential mode
  protected TaskAttemptContext sequentialTask; // task attempt for sequential mode

  @Override
  public void init(JsonValue args) throws Exception
  {
    init((JsonRecord)args);
  }

  /**
   * @param args
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  protected void init(JsonRecord args) throws Exception
  {
    this.args = new BufferedJsonRecord(args.size());
    this.args.setCopy(args);

    // set the location
    this.location = AdapterStore.getStore().getLocation(args);

    // set the options
    this.options = AdapterStore.getStore().output.getOption(args);

    // set the format
    this.oFormat = (OutputFormat<?,?>) AdapterStore.getStore().getClassFromRecord(
        options, FORMAT_NAME, null).newInstance();

    // set the configurator
    Class<?> configuratorClass = AdapterStore.getStore().getClassFromRecord(
        options, CONFIGURATOR_NAME, null);
    if (configuratorClass != null)
    {
      this.configurator = (InitializableConfSetter) configuratorClass.newInstance();
      this.configurator.init(args);            
    }

    // set the converter
    Class<?> converterClass = AdapterStore.getStore().getClassFromRecord(options,
        CONVERTER_NAME, null);
    if (converterClass != null)
    {
      this.converter = (KeyValueExport<K,V>) converterClass.newInstance();
      this.converter.init(options);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.StorableAdapter#open()
   */
  public void open() throws Exception
  {
    this.conf = new JobConf();
    this.reporter = Reporter.NULL;
    
    // Some OutputFormats (like FileOutputFormat) require that the job id/task id set.
    // So let's set it for all output formats, just in case they need it too.
    JobID jobid = new JobID("sequential", jobCounter.getAndIncrement());
    TaskAttemptID taskid = new TaskAttemptID(new TaskID(jobid, true, 0),0);
    conf.set("mapred.task.id", taskid.toString());

    setSequential(conf);
    
    // Create a task so we can use committers.
    sequentialJob = new ExposeJobContext(conf, jobid);
    sequentialTask = new ExposeTaskAttemptContext(conf, taskid);

    // Give the commiter a chance initialize.
    OutputCommitter committer = conf.getOutputCommitter();
    // FIXME: We skip job setup for now because  
    committer.setupJob(sequentialJob);
    committer.setupTask(sequentialTask);

    if (oFormat instanceof JobConfigurable)
      ((JobConfigurable) oFormat).configure(conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.StorableAdapter#close()
   */
  public void close() throws Exception
  {
    if (writer != null)
    {
      writer.close((Reporter) reporter);
      writer = null;
      OutputCommitter committer = conf.getOutputCommitter();
      committer.commitTask(sequentialTask);
      // FIXME: We skip the job cleanup because the FileOutputCommitter deletes the _temporary, which
      // is shared by all sequential jobs.
      committer.cleanupJob(sequentialJob);
      sequentialTask = null;
//      
//      if (committer instanceof FileOutputCommitter) {
//       
//        // for this case, only one file is expected
//        String fileName = new Path(location).getName();
//        Path pTgt = null;
//        Path src = null;
//        try {
//          pTgt = FileOutputFormat.getOutputPath(conf);
//          src = FileOutputFormat.getTaskOutputPath(conf, fileName);
//        } catch(Exception e) {
//          // TODO: this can happen if the OutputFormat is not a FileOutputFormat,
//          // i.e., for HBase
//          LOG.warn("task output files not found");
//        }
//        if(pTgt != null && src != null) {
//          Path tgt = new Path(FileOutputFormat.getOutputPath(conf), fileName);
//
//          FileSystem fs = src.getFileSystem(conf);
//          if(fs.exists(tgt)) {
//            fs.delete(tgt, true);
//          }
//
//
//          // rename src to tgt
//          fs.rename(src, tgt);
//
//          // clean-up the temp
//          Path tmp = new Path(FileOutputFormat.getOutputPath(conf), FileOutputCommitter.TEMP_DIR_NAME);
//          if(fs.exists(tmp))
//            fs.delete(tmp, true);
//        }
//      }
    }
  }

  protected JsonHolder keyHolder()
  {
    return new JsonHolderDefault();
  }

  protected JsonHolder valueHolder()
  {
    return new JsonHolderDefault();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.OutputAdapter#getRecordWriter()
   */
  public ClosableJsonWriter getWriter() throws Exception
  {
    Path lPath = new Path(location);

    final RecordWriter<JsonHolder, JsonHolder> writer = getRecordWriter(FileSystem
        .get(conf), conf, lPath.getName(), reporter);
    return new ClosableJsonWriter() {
      JsonHolder keyHolder = keyHolder();
      JsonHolder valueHolder = valueHolder();

      public void flush() throws IOException
      {
        // TODO: hmm... RecordWriter has no flush method... 
      }

      public void close() throws IOException
      {
        writer.close(null);
      }

      public void write(JsonValue value) throws IOException
      {
        valueHolder.value = value;
        writer.write(keyHolder, valueHolder);
      }
    };
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.OutputFormat#getRecordWriter(org.apache.hadoop.fs.FileSystem,
   *      org.apache.hadoop.mapred.JobConf, java.lang.String,
   *      org.apache.hadoop.util.Progressable)
   */
  @SuppressWarnings("unchecked")
  public RecordWriter<JsonHolder, JsonHolder> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) throws IOException
  {
    if (converter == null)
    {
      final RecordWriter<JsonHolder, JsonHolder> baseWriter 
        = ((OutputFormat<JsonHolder, JsonHolder>) oFormat).getRecordWriter(ignored,
          job, name, progress);
      final JsonHolder nullHolder = keyHolder();
      writer = new RecordWriter<JsonHolder, JsonHolder>() {
        public void close(Reporter reporter) throws IOException
        {
          baseWriter.close(reporter);
        }

        public void write(JsonHolder key, JsonHolder value) throws IOException
        {
          baseWriter.write(nullHolder, value); // key is unused
        }
      };
    }
    else
    {
      final RecordWriter<K, V> baseWriter = ((OutputFormat<K, V>) oFormat)
          .getRecordWriter(ignored, job, name, progress);

      final K baseKey = converter.createKeyTarget();
      final V baseValue = converter.createValueTarget();

      writer = new RecordWriter<JsonHolder, JsonHolder>() {

        public void close(Reporter reporter) throws IOException
        {
          baseWriter.close(reporter);
        }

        public void write(JsonHolder key, JsonHolder value) throws IOException
        {
          converter.convert(value.value, baseKey, baseValue);
          baseWriter.write(baseKey, baseValue);
        }
      };
    }
    return writer;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.OutputFormat#checkOutputSpecs(org.apache.hadoop.fs.FileSystem,
   *      org.apache.hadoop.mapred.JobConf)
   */
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
      throws IOException
  {
    oFormat.checkOutputSpecs(ignored, job);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.JobConfigurable#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf conf)
  {
    Globals.setJobConf(conf);
    if (oFormat == null)
    {
      try
      {
        JsonRecord r = ConfUtil.readConf(conf, ConfSetter.CONFOUTOPTIONS_NAME);
        init(r);
      }
      catch (Exception e)
      {
        throw new RuntimeException(e);
      }
    }
    if (oFormat instanceof JobConfigurable)
    {
      ((JobConfigurable) oFormat).configure(conf);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#setSequential(org.apache.hadoop.mapred.JobConf)
   */
  public void setSequential(JobConf conf) throws Exception
  {
    set(conf);
    configurator.setSequential(conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#setParallel(org.apache.hadoop.mapred.JobConf)
   */
  public void setParallel(JobConf conf) throws Exception
  {
    set(conf);
    configurator.setParallel(conf);
  }

  /**
   * @param conf
   * @throws Exception
   */
  protected void set(JobConf conf) throws Exception
  {
    conf.setOutputFormat(this.getClass());
    // add options to args
    AdapterStore.getStore().output.replaceOption(args, options);
    // Add any conf overrides from the options into the conf
    ConfUtil.writeConfOptions(conf, options);    
    // write out args and options to the conf
    ConfUtil.writeConf(conf, ConfSetter.CONFOUTOPTIONS_NAME, args);
    Globals.setJobConf(conf);
  }
  
  @Override
  public JsonValue expand() throws Exception{
	  BufferedJsonRecord jr = (BufferedJsonRecord)this.options.getCopy(null);
	  BufferedJsonArray ja = new BufferedJsonArray();
	  String namenode = null;
	  String dfsport = null;
	  if (this.location != null) {			
			Configuration conf = new Configuration();
			namenode = conf.get("fs.default.name");			
			dfsport = conf.get("dfs.namenode.http-address");								
			FileSystem fs = FileSystem.get(conf);								
			FileStatus files = fs.getFileStatus(new Path(this.location));
			
			if (files.isDir()) {
				StringBuilder sb = new StringBuilder();
				FileStatus[] dirContent = fs.listStatus(new Path(this.location));
				for (FileStatus file : dirContent) {
					if (!file.isDir()) {
						ja.add(new JsonString(file.getPath().toString()));
					}
				}				
			} else {
				ja.add(new JsonString(files.getPath().toString()));
			}
		}
	  jr.add(Adapter.LOCATION_NAME, ja);
	  jr.add(Adapter.TYPE_NAME, args.get(Adapter.TYPE_NAME));
	  jr.add(new JsonString("expanded"), JsonBool.make(true));
	  
	  if(namenode != null)
		  jr.add(new JsonString("fs.default.name"), new JsonString(namenode));
	  if(dfsport != null)
		  jr.add(new JsonString("dfs.namenode.http-address"), new JsonString(dfsport));
	  
	  return jr;
  }
}
