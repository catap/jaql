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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.io.hadoop.converter.KeyValueExport;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.BufferedJsonRecord;

/**
 * The default class for writing Items from jaql to Hadoop
 */
public class DefaultHadoopOutputAdapter<K,V> implements HadoopOutputAdapter
{
  static final Logger                LOG    = Logger.getLogger(DefaultHadoopOutputAdapter.class.getName());

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
  private void init(JsonRecord args) throws Exception
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
    this.reporter = new Reporter() {
      public void incrCounter(String arg0, String arg1, long arg2)
      {        
      }
      
      public void setStatus(String status)
      {
      }

      public void progress()
      {
      }

      @SuppressWarnings("unchecked")
      public void incrCounter(Enum key, long amount)
      {
      }

      public InputSplit getInputSplit() throws UnsupportedOperationException
      {
        return null;
      }
    };

    configurator.setSequential(conf);
    Globals.setJobConf(conf);
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
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.OutputAdapter#getRecordWriter()
   */
  public ClosableJsonWriter getJsonWriter() throws Exception
  {
    Path lPath = new Path(location);

    final RecordWriter<JsonHolder, JsonHolder> writer = getRecordWriter(FileSystem
        .get(conf), conf, lPath.getName(), reporter);
    return new ClosableJsonWriter() {
      JsonHolder keyHolder = new JsonHolder();
      JsonHolder valueHolder = new JsonHolder();
      
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
      writer = ((OutputFormat<JsonHolder, JsonHolder>) oFormat).getRecordWriter(ignored,
          job, name, progress);
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
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#setParallel(org.apache.hadoop.mapred.JobConf)
   */
  public void setParallel(JobConf conf) throws Exception
  {
    set(conf);
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
    // write out args and options to the conf
    ConfUtil.writeConf(conf, ConfSetter.CONFOUTOPTIONS_NAME, args);
    configurator.setParallel(conf);
    Globals.setJobConf(conf);
  }
}
