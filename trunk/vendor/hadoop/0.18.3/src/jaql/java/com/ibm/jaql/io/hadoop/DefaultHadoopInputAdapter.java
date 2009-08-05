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

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.hadoop.converter.KeyValueImport;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;

/**
 * The default class for reading data from Hadoop into jaql
 */
public class DefaultHadoopInputAdapter<K,V> implements HadoopInputAdapter
{
  static final Logger          LOG = Logger.getLogger(DefaultHadoopInputAdapter.class.getName());

  protected InputFormat<K,V>        iFormat;

  protected InitializableConfSetter     configurator;

  protected KeyValueImport<K, V> converter;

  protected JobConf            conf;

  protected Reporter           reporter;

  protected BufferedJsonRecord      args;

  protected String             location;

  protected BufferedJsonRecord      options;
  
  public void init(JsonValue args) throws Exception {
    initializeFrom((JsonRecord)args);
  }
  
  /**
   * @param args
   * @throws Exception
   */
  @SuppressWarnings("unchecked")
  protected void initializeFrom(JsonRecord args) throws Exception
  {
    this.args = (BufferedJsonRecord) args;

    // set the location
    this.location = AdapterStore.getStore().getLocation(args);

    // set the options
    this.options = AdapterStore.getStore().input.getOption(args);

    // set the format
    this.iFormat = (InputFormat<K,V>) AdapterStore.getStore().getClassFromRecord(
        options, FORMAT_NAME, null).newInstance();

    // set the configurator
    Class<?> configuratorClass = AdapterStore.getStore().getClassFromRecord(
        options, CONFIGURATOR_NAME, null);
    if (configuratorClass != null)
    {
      this.configurator = (InitializableConfSetter) configuratorClass.newInstance();
      this.configurator.init(args); // FIXME: no need to "new"
    }

    // set the converter
    Class<?> converterClass = AdapterStore.getStore().getClassFromRecord(options,
        CONVERTER_NAME, null);
    if (converterClass != null)
    {
      this.converter = (KeyValueImport<K, V>) converterClass.newInstance();
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
    Globals.setJobConf(conf);
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

    // write state to conf, pass in top-level args
    setSequential(conf);
    configurator.setSequential(conf);
    Globals.setJobConf(conf);
    // initialize the format from conf
    if (iFormat instanceof JobConfigurable)
      ((JobConfigurable) iFormat).configure(conf);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.StorableAdapter#close()
   */
  public void close() throws Exception
  {
    // do nothing
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.mapred.JobConf,
   *      int)
   */
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException
  {
    // return splits
    return iFormat.getSplits(job, numSplits);
  }

  /**
   * 
   */
  private class SplitState
  {
    int                      splitId = 0;

    RecordReader<JsonHolder, JsonHolder> reader  = null;
  };

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.InputAdapter#getRecordReader()
   */
  public ClosableJsonIterator iter() throws IOException
  {
    final SplitState state = new SplitState();
    final InputSplit[] splits = getSplits(conf, conf.getNumMapTasks());

    final JsonHolder valueHolder = valueHolder();
    valueHolder.value = converter != null ? converter.createTarget() : null;
    return new ClosableJsonIterator() {
      JsonHolder key;
      /*
       * (non-Javadoc)
       * 
       * @see com.ibm.jaql.io.ItemReader#close()
       */
      @Override
      public void close() throws IOException
      {
        if (state.reader != null)
        {
          state.reader.close();
        }
      }

      /*
       * (non-Javadoc)
       * 
       * @see com.ibm.jaql.io.ItemReader#next(com.ibm.jaql.json.type.Item)
       */
      public boolean moveNext() throws IOException
      {
        while (true)
        {
          if (state.reader == null)
          {
            if (state.splitId >= splits.length)
            {
              close();
              return false;
            }
            InputSplit split = splits[state.splitId++];
            state.reader = getRecordReader(split, conf, reporter);
            if (key == null) key = state.reader.createKey();
          }
          if (state.reader.next(key, valueHolder))
          {
            currentValue = valueHolder.value;
            return true;
          }
          state.reader.close();
          state.reader = null;
        }
      }
    };
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
   * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit,
   *      org.apache.hadoop.mapred.JobConf, org.apache.hadoop.mapred.Reporter)
   */
  @SuppressWarnings("unchecked")
  public RecordReader<JsonHolder, JsonHolder> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException
  {
    if (converter == null)
      return ((InputFormat<JsonHolder, JsonHolder>) iFormat)
          .getRecordReader(split, job, reporter);
    final RecordReader<K, V> baseReader = ((InputFormat<K, V>) iFormat)
        .getRecordReader(split, job, reporter);
    final K baseKey = baseReader.createKey();
    final V baseValue = baseReader.createValue();

    return new RecordReader<JsonHolder, JsonHolder>() {

      public void close() throws IOException
      {
        baseReader.close();
      }

      public JsonHolder createKey()
      {
        return keyHolder();
      }

      public JsonHolder createValue()
      {
        JsonHolder holder = valueHolder();
        holder.value = converter.createTarget();
        return holder;
      }

      public long getPos() throws IOException
      {
        return baseReader.getPos();
      }

      public float getProgress() throws IOException
      {
        return baseReader.getProgress();
      }

      public boolean next(JsonHolder key, JsonHolder value) throws IOException
      {
        boolean hasMore = baseReader.next(baseKey, baseValue);
        if (!hasMore) return false;
        value.value = converter.convert(baseKey, baseValue, value.value);
        return true;
      }
    };
  }



  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.JobConfigurable#configure(org.apache.hadoop.mapred.JobConf)
   */
  public void configure(JobConf conf)
  {
    Globals.setJobConf(conf);
    // TODO: factor this configuration code so that it can be shared with the
    // composite input format...
    // setup the internal input format
    if (iFormat == null)
    {
      try
      {
        JsonRecord options = ConfUtil
            .readConf(conf, ConfSetter.CONFINOPTIONS_NAME);
        initializeFrom(options);
      }
      catch (Exception e)
      {
        throw new RuntimeException(
            "Could not initialize InputAdapter from JobConf", e);
      }
    }
    if (iFormat instanceof JobConfigurable)
    {
      ((JobConfigurable) iFormat).configure(conf);
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
    // TODO: refactor so that its shared with composite inputformat...
    // set the input format to this adapter
    conf.setInputFormat(this.getClass());

    // replace the options record in args with the options override
    AdapterStore.getStore().input.replaceOption(args, options);

    // write the top-level args for the adapter
    ConfUtil.writeConf(conf, ConfSetter.CONFINOPTIONS_NAME, args);

    // write the optional args for the configurator
    configurator.setParallel(conf); // TODO: double-check what options the
    // configurator has at this point
    Globals.setJobConf(conf);
  }
  
  @Override
  public Schema getSchema()
  {
    if (converter != null) // input file is already Json
    {
      return new ArraySchema(converter.getSchema(), null, null);
    }
    else
    {
      return SchemaFactory.arraySchema();      
    }
  }

  @Deprecated
  @Override
  public void validateInput(JobConf job) throws IOException
  {
    // check the input format
    InputFormat<?,?> adapter = job.getInputFormat();
    if (!(adapter instanceof HadoopInputAdapter))
      throw new IOException("invalid input format: " + adapter);
  }
}
