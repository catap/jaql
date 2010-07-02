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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * When run, recursively delete the path from the filesystem.  
 */
public class DeleteFileTask implements Runnable
{
  protected FileSystem fs;
  protected Path path;
  
  public DeleteFileTask(FileSystem fs, Path path)
  {
    this.fs = fs;
    this.path = path;
  }

  @Override
  public void run()
  {
    try
    {
      if( fs.exists(path) ) 
      {
        fs.delete(path, true);
      }
    }
    catch(IOException e)
    {
      throw new RuntimeException(e);
    }
  }
}
