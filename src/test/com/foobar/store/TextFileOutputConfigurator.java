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
package com.foobar.store;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.hadoop.FileOutputConfigurator;
import com.ibm.jaql.json.type.MemoryJRecord;

/**
 * 
 */
public class TextFileOutputConfigurator extends FileOutputConfigurator
{
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.FileOutputConfigurator#setSequential(org.apache.hadoop.mapred.JobConf)
   */
  @Override
  public void setSequential(JobConf conf) throws Exception
  {
    super.setSequential(conf);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.FileOutputConfigurator#setParallel(org.apache.hadoop.mapred.JobConf)
   */
  @Override
  public void setParallel(JobConf conf) throws Exception
  {
    super.setParallel(conf);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
  }
}
