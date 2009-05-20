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
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

/** Input format that reads an {@link JsonArray} from Hadoop's job configuration and creates
 * a single split for each element of that array. */
public class ArrayInputFormat implements InputFormat<JsonHolder, JsonHolder>
{

  public static final String ARRAY_NAME     = "array";

  public static final String JOB_ARRAY_NAME = ArrayInputFormat.class.getCanonicalName() + ".array";

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit,
   *      org.apache.hadoop.mapred.JobConf, org.apache.hadoop.mapred.Reporter)
   */
  public RecordReader<JsonHolder, JsonHolder> getRecordReader(InputSplit split,
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
    JsonArray arr = null;
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
class ArrayRecordReader implements RecordReader<JsonHolder, JsonHolder>
{

  private JsonValue splitValue;

  private boolean seen = false;

  /**
   * @param value
   */
  public ArrayRecordReader(JsonValue value)
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
  public JsonHolder createKey()
  {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.mapred.RecordReader#createValue()
   */
  public JsonHolder createValue()
  {
    return new JsonHolder();
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
  public boolean next(JsonHolder key, JsonHolder value) throws IOException
  {
    if (!seen)
    {
      try
      {
        value.value = JsonValue.getCopy(splitValue, value.value);
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
  private BinaryFullSerializer serializer = DefaultBinaryFullSerializer.getInstance();
  private JsonValue value;

  /**
   * 
   */
  public ArrayInputSplit()
  {
    value = null;
  }

  /**
   * @param v
   */
  public ArrayInputSplit(JsonValue v)
  {
    value = v;
  }

  /**
   * @return
   */
  public JsonValue getValue()
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
    value = serializer.read(in, value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  public void write(DataOutput out) throws IOException
  {
    serializer.write(out, value);
  }

}

/**
 * 
 */
class ArrayInputConfigurator implements InitializableConfSetter
{

  protected JsonRecord options;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#init(java.lang.Object)
   */
  public void init(JsonValue data) throws Exception
  {
    options = AdapterStore.getStore().input.getOption((JsonRecord) data);
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
    JsonArray data = (JsonArray) options.getValue(ArrayInputFormat.ARRAY_NAME);
    ConfUtil.writeConfArray(conf, ArrayInputFormat.JOB_ARRAY_NAME, data);
  }

}
