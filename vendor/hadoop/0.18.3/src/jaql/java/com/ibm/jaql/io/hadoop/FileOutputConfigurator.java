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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;

/**
 * A Configurator that specifically writes the JobConf for OutputFormat
 */
public class FileOutputConfigurator implements JSONConfSetter
{
  protected String location;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#init(java.lang.Object)
   */
  public void init(Item options) throws Exception
  {
    location = AdapterStore.getStore().getLocation((JRecord) options.get());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#setSequential(org.apache.hadoop.mapred.JobConf)
   */
  public void setSequential(JobConf conf) throws Exception
  {
    conf.setOutputKeyClass(Item.class);
    conf.setOutputValueClass(Item.class);
    HadoopSerialization.register(conf);
    
    // For an expression, the location is the final file name, so its directory
    // must be the location's parent.
    Path outPath = new Path(location);
    FileSystem fs = outPath.getFileSystem(conf);
    if (fs.exists(outPath) && fs.isFile(outPath)) fs.delete(outPath);

    conf.setOutputPath(outPath.getParent());
    // HACK: copied from FileOutputFormat since it is package protected.
    Path workOutputDir = new Path(conf.getWorkingDirectory(), outPath);
    conf.set("mapred.work.output.dir", workOutputDir.toString());
    if (!fs.exists(workOutputDir))
    {
      fs.mkdirs(workOutputDir);
    }
    
    Path tempDir = new Path(outPath.getParent(), "_temporary");
    if (!fs.exists(tempDir))
    {
      fs.mkdirs(tempDir);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.ConfSetter#setParallel(org.apache.hadoop.mapred.JobConf)
   */
  public void setParallel(JobConf conf) throws Exception
  {
    conf.setOutputKeyClass(Item.class);
    conf.setOutputValueClass(Item.class);
    // TODO: currently assumes usage of  FullSerializer#getDefault()
    HadoopSerialization.register(conf);

    // For map-reduce, multiple files can be produced, so the location is their
    // parent directory.
    Path outPath = new Path(location);
    outPath.getFileSystem(conf).delete(outPath);
    conf.setOutputPath(outPath);
    // HACK: copied from FileOutputFormat since it is package protected.
    Path workOutputDir = new Path(conf.getWorkingDirectory(), outPath);
    conf.set("mapred.work.output.dir", workOutputDir.toString());
  }
}
