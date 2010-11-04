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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.hadoop.converter.KeyValueImport;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * The default class for reading data from Hadoop into jaql
 */
public class DefaultHadoopInputAdapter<K,V> implements HadoopInputAdapter
{
  static final Logger          LOG = Logger.getLogger(DefaultHadoopInputAdapter.class.getName());

  public static final JsonString PRESERVE_ORDER_OPTION = new JsonString("ordered");

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
    this.args = (BufferedJsonRecord) args; // FIXME: shouldn't this be just (JsonRecord)? Who owns it? (ksb)

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
    if(Globals.getJobConf() != null)
      conf.setWorkingDirectory(Globals.getJobConf().getWorkingDirectory());
    
    this.reporter = Reporter.NULL;    

    // write state to conf, pass in top-level args
    setSequential(conf);
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
    InputSplit[] inSplits = iFormat.getSplits(job, numSplits);
    if( JaqlUtil.ebv( this.options.get(PRESERVE_ORDER_OPTION) ))
    {
      // TODO: only do if order preserving?  Need to fix fileSplitToRecord
      InputSplit[] splits = new InputSplit[inSplits.length];
      for(int i = 0 ; i < splits.length ; i++)
      {
        splits[i] = new DHIASplit(inSplits[i], i);
      }
      return splits;
    }
    return inSplits;
  }

  /**
   * A wrapper for the real input split.  At this point, this wrapper is solely used to prevent
   * JobClient.writeNewSplits() from reordering our splits because it makes map jobs non-order preserving.
   */
  public static class DHIASplit implements InputSplit
  {
    protected InputSplit split;
    protected int index;
    
    public DHIASplit()
    {
    }

    public DHIASplit(InputSplit split, int index)
    {
      this.split = split;
      this.index = index;
    }

    @Override
    public long getLength() throws IOException
    {
      // We lie to hadoop so it doesn't reorder our tasks by size.  We prefer require
      // order preserved.
      // TODO: open two bugs against hadoop:
      //   1. JobClient.writeNewSplits should NOT reorder splits. Instead the job scheduler
      //      could schedule tasks by size if it so chooses.
      //   2. RawLocalFileSystem.listStatus should sort files returned by File.list() by name
      //      to preserve order.
      return Integer.MAX_VALUE - index;
    }

    @Override
    public String[] getLocations() throws IOException
    {
      return split.getLocations();
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
      String cname = in.readUTF();
      try
      {
        Class<? extends InputSplit> cls = Class.forName(cname).asSubclass(InputSplit.class);
        split = (InputSplit)ReflectionUtils.newInstance(cls, null);
      } 
      catch (Exception e) 
      {
        throw new IOException("Failed split init", e);
      }
      index = in.readInt();
      split.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeUTF(split.getClass().getName());
      out.writeInt(index);
      split.write(out);
    }    
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
    if( split instanceof DHIASplit )
    {
      // not using order-preserving wrapper
      split = ((DHIASplit)split).split;
    }
    
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
    
    // write the optional args for the configurator
    if (configurator != null)
    {
      configurator.setSequential(conf); // TODO: double-check what options the
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#setParallel(org.apache.hadoop.mapred.JobConf)
   */
  public void setParallel(JobConf conf) throws Exception
  {
    set(conf);
    
    if (configurator != null)
    {
      // write the optional args for the configurator
      configurator.setParallel(conf); // TODO: double-check what options the
    }
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

    // Add any conf overrides from the options into the conf
    ConfUtil.writeConfOptions(conf, options);
    
    // write the top-level args for the adapter
    ConfUtil.writeConf(conf, ConfSetter.CONFINOPTIONS_NAME, args);
    
    // Set the global options
    Globals.setJobConf(conf);
  }
  
  @Override
  public Schema getSchema()
  {
    if (converter != null) // input file is already Json
    {
      return new ArraySchema(null, converter.getSchema());
    }
    else
    {
      return SchemaFactory.arraySchema();      
    }
  }

}
