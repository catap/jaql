/*
 * Copyright (C) IBM Corp. 2010.
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

/**
 * A OutputCommitter that works with FileOutputFormats in sequential mode.
 * It avoids extra directories and file moves of the standard FileOutputCommitter.
 */
public class DirectFileOutputCommiter extends OutputCommitter
{
  @Override
  public void setupJob(JobContext context) throws IOException
  {
    // Create the path to the file, if needed.
    JobConf conf = context.getJobConf();
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    if (outputPath != null) {
      Path tmpDir = outputPath.getParent();
      FileSystem fileSys = outputPath.getFileSystem(conf);
      if (!fileSys.mkdirs(outputPath.getParent())) {
        throw new IOException("Mkdirs failed to create " + tmpDir.toString());
      }
    }
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException
  {
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException 
  {
  }
          
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException
  {
    return false;
  }
  
  @Override
  public void commitTask(TaskAttemptContext context) throws IOException
  {
    // Nothing to do becuase we wrote directly to the file.
  }
          
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException
  {
    Path outputPath = FileOutputFormat.getOutputPath(context.getJobConf());
    // TODO: LOG.warn("partial write of "+outputPath+"  What should we do with partial output when writing directly?");
//    Path taskOutputPath =  getTempTaskOutputPath(context);
//    try {
//      if (taskOutputPath != null) {
//        FileSystem fs = taskOutputPath.getFileSystem(context.getJobConf());
//        context.getProgressible().progress();
//        fs.delete(taskOutputPath, true);
//      }
//    } catch (IOException ie) {
//      LOG.warn("Error discarding output" + StringUtils.stringifyException(ie));
//    }
  }
}
