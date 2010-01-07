/**
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
package com.ibm.jaql.io.hadoop;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;



/**
 * Reads a single split from an input format
 */
public class SelectSplitInputFormat<K,V> implements InputFormat<K,V>, JobConfigurable
{
  public final static String BASE_NAME = SelectSplitInputFormat.class.getName();
  public final static String INPUT_FORMAT = BASE_NAME + ".input.format";
  public final static String SPLIT_CLASS = BASE_NAME + ".split.class";
  public final static String SPLIT = BASE_NAME + ".split.data";
  public final static String STATE = BASE_NAME + ".state";


  protected InputFormat<K,V> iFormat;
  protected InputSplit split;

  @Override
  public RecordReader<K,V> getRecordReader(
      InputSplit split, JobConf conf, Reporter reporter) throws IOException
  {
    return iFormat.getRecordReader(split, conf, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException
  {
    return new InputSplit[]{ split };
  }

  @Override
  public void configure(JobConf conf)
  {
    Class<? extends InputFormat> inputFormatCls = conf.getClass(INPUT_FORMAT, null, InputFormat.class);
    iFormat = (InputFormat<K,V>)ReflectionUtils.newInstance(inputFormatCls, conf);
    Class<? extends InputSplit> splitCls = conf.getClass(SPLIT_CLASS, null, InputSplit.class);
    split = (InputSplit)ReflectionUtils.newInstance(splitCls, conf);
    byte[] bytes = ConfUtil.readBinary(conf, SPLIT);      
    DataInputBuffer buffer = new DataInputBuffer();
    buffer.reset(bytes, bytes.length);
    try
    {
      split.readFields(buffer);
    }
    catch (IOException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }
  
  @Deprecated
  @Override
  public void validateInput(JobConf job) throws IOException
  {
    Class<? extends InputFormat> inputFormatCls = job.getClass(INPUT_FORMAT, null, InputFormat.class);
    iFormat = (InputFormat<K,V>)ReflectionUtils.newInstance(inputFormatCls, job);

    JobConf job2 = new JobConf(job);
    job2.setInputFormat(inputFormatCls); // Just to be safe
    iFormat.validateInput(job2);
  }
}
