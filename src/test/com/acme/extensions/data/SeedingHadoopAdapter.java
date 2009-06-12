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
package com.acme.extensions.data;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.MemoryJRecord;

/**
 * 
 */
public class SeedingHadoopAdapter extends DefaultHadoopInputAdapter
{

  private long   seed;
  private Random rng;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter#initializeFrom(com.ibm.jaql.json.type.Item)
   */
  @Override
  public void initializeFrom(Item item) throws Exception
  {
    super.initializeFrom(item);
    seed = ((JLong) options.getValue("seed").get()).value;
    rng = new Random(seed);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter#set(org.apache.hadoop.mapred.JobConf)
   */
  @Override
  protected void set(JobConf conf) throws Exception
  {
    super.set(conf);
    //  set the input format to this adapter
    conf.setInputFormat(this.getClass());
    conf.set("seed", String.valueOf(seed));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter#configure(org.apache.hadoop.mapred.JobConf)
   */
  @Override
  public void configure(JobConf conf)
  {
    super.configure(conf);
    seed = Long.parseLong(conf.get("seed"));
    rng = new Random(seed);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter#getRecordReader(org.apache.hadoop.mapred.InputSplit,
   *      org.apache.hadoop.mapred.JobConf, org.apache.hadoop.mapred.Reporter)
   */
  @Override
  public RecordReader<Item, Item> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException
  {

    job.set("seed", String.valueOf(((SeededSplit) split).getSeed()));
    return super.getRecordReader(((SeededSplit) split).getChildSplit(), job,
        reporter);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter#getSplits(org.apache.hadoop.mapred.JobConf,
   *      int)
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException
  {
    // TODO Auto-generated method stub
    InputSplit[] childSplits = super.getSplits(job, numSplits);
    SeededSplit[] splits = new SeededSplit[childSplits.length];
    for (int i = 0; i < splits.length; i++)
    {
      splits[i] = new SeededSplit(childSplits[i], rng.nextLong());
    }
    return splits;
  }
}
