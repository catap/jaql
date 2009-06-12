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
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.util.BaseUtil;

/**
 * 
 */
public class ArrayInputFormat implements InputFormat<Item, Item>
{

  public static final String ARRAY_NAME     = "array";

  public static final String JOB_ARRAY_NAME = ArrayInputFormat.class.getCanonicalName() + ".array";

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit,
   *      org.apache.hadoop.mapred.JobConf, org.apache.hadoop.mapred.Reporter)
   */
  public RecordReader<Item, Item> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException
  {
    return new ArrayRecordReader(((ArrayInputSplit) split).getValue());
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.mapred.JobConf,
   *      int)
   */
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException
  {
    // get the JArray of Items from the conf
    JArray arr = null;
    try
    {
      arr = ConfUtil.readConfArray(job, JOB_ARRAY_NAME);
    }
    catch (Exception e)
    {
      BaseUtil.LOG.warn("array read failed", e);
      throw new RuntimeException(e);
    }
    // make a split for each one
    InputSplit[] splits = null;
    try
    {
      splits = new InputSplit[(int) arr.count()]; // FIXME: should not require a
      // cast here...
      for (int i = 0; i < splits.length; i++)
      {
        splits[i] = new ArrayInputSplit(arr.nth(i));
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }

    return splits;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputFormat#validateInput(org.apache.hadoop.mapred.JobConf)
   */
  public void validateInput(JobConf job) throws IOException
  {
    // verify that an array exists
    if (job.get(JOB_ARRAY_NAME) == null)
      throw new IOException("variable name: " + JOB_ARRAY_NAME + ", not set");
  }
}

/**
 * 
 */
class ArrayRecordReader implements RecordReader<Item, Item>
{

  private Item    splitValue;

  private boolean seen = false;

  /**
   * @param value
   */
  public ArrayRecordReader(Item value)
  {
    this.splitValue = value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#close()
   */
  public void close() throws IOException
  {
    // nothing
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#createKey()
   */
  public Item createKey()
  {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#createValue()
   */
  public Item createValue()
  {
    return new Item();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#getPos()
   */
  public long getPos() throws IOException
  {
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#getProgress()
   */
  public float getProgress() throws IOException
  {
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object,
   *      java.lang.Object)
   */
  public boolean next(Item key, Item value) throws IOException
  {
    if (!seen)
    {
      try
      {
        value.copy(splitValue);
        seen = true;
        return true;
      }
      catch (IOException e)
      {
        throw e;
      }
      catch (Exception e)
      {
        throw new UndeclaredThrowableException(e);
      }
    }
    return false;
  }
}

/**
 * 
 */
class ArrayInputSplit implements InputSplit
{

  private Item value;

  /**
   * 
   */
  public ArrayInputSplit()
  {
    value = new Item();
  }

  /**
   * @param v
   */
  public ArrayInputSplit(Item v)
  {
    value = v;
  }

  /**
   * @return
   */
  public Item getValue()
  {
    return value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputSplit#getLength()
   */
  public long getLength() throws IOException
  {
    return 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputSplit#getLocations()
   */
  public String[] getLocations() throws IOException
  {
    return new String[0]; // FIXME: use a static
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException
  {
    value.readFields(in);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException
  {
    value.write(out);
  }

}

/**
 * 
 */
class ArrayInputConfigurator implements JSONConfSetter
{

  protected JRecord options;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#init(java.lang.Object)
   */
  public void init(Item data) throws Exception
  {
    options = AdapterStore.getStore().input.getOption((JRecord) data.get());
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
    JArray data = (JArray) options.getValue(ArrayInputFormat.ARRAY_NAME).get();
    ConfUtil.writeConfArray(conf, ArrayInputFormat.JOB_ARRAY_NAME, data);
  }

}
