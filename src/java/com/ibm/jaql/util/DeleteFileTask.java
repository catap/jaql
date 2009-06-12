/*
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
package com.ibm.jaql.util;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class DeleteFileTask implements Runnable
{
  protected Path path;
  
  public DeleteFileTask(Path path)
  {
    this.path = path;
  }

  public DeleteFileTask(String path) throws URISyntaxException
  {
    this.path = new Path(path);
  }

  // fails on java 1.5: @Override
  public void run()
  {
    try
    {
      Configuration conf = new JobConf(); // TODO: where to get this from?
      FileSystem fs = path.getFileSystem(conf);
      if( fs.exists(path) ) 
      {
        fs.delete(path);
      }
    }
    catch(IOException e)
    {
      throw new RuntimeException(e);
    }
  }
}
