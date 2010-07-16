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
package com.ibm.jaql.fail.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.util.ReflectionUtils;

public class ErrorInputFormat extends SequenceFileInputFormat<LongWritable, ErrorWritable>
{
  public static String ERROR_NAME = "ErrorInputFormat.code";
  public static String ERROR_NEXT_MAX = "ErrorInputFormat.nextMax";

  public enum Error { NONE, SPLIT, OPEN, BOGUS_SPLIT, NEXT, CLOSE };

  public ErrorInputFormat() {
    super();
  }

  @Override
  public RecordReader<LongWritable, ErrorWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    // fail on OPEN
    String val = job.get(ERROR_NAME);
    Error err = Error.valueOf(val);
    if(err.equals(Error.OPEN)) {
      throw new IOException("Intentional error on open");
    }

    return new ErrorRecordReader(job, (ErrorSplit)split);
  }

  @Override
  public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
    String val = arg0.get(ERROR_NAME);
    Error err = Error.valueOf(val);		
    // fail on SPLIT,
    if(err.equals(Error.SPLIT)) {
      throw new IOException("Intentional error on split");
    }

    int nextCount = arg0.getInt(ERROR_NEXT_MAX, 1);
    
    // get the original splits
    InputSplit[] splits = super.getSplits(arg0, arg1);
    int len = splits.length;
    ArrayList<InputSplit> newSplits = new ArrayList<InputSplit>(len + 1);
    for(int i = 0; i < len; i++) {
      newSplits.add(new ErrorSplit((FileSplit)splits[i], arg0, err, nextCount));
    }
    // generate bogus split for BOGUS_SPLIT
    if(err.equals(Error.BOGUS_SPLIT)) {

      newSplits.add(new ErrorSplit(new FileSplit(new Path("/bogus/file"), 0, 10, arg0), arg0, Error.NONE, nextCount));
    }

    return newSplits.toArray(new InputSplit[newSplits.size()]);
  }

  public class ErrorRecordReader extends SequenceFileRecordReader<LongWritable, ErrorWritable>
  {
    Error err;
    int errMax;
    private int errNum = 0;

    public ErrorRecordReader(Configuration conf, ErrorSplit split) throws IOException {
      super(conf, (FileSplit)split.child);
      // extract the error code from split (expect ErrorSplit)
      err = split.error;
      errMax = split.count;
    }

    @Override
    public synchronized void close() throws IOException {
      super.close();
      if(err.equals(Error.CLOSE))
	throw new IOException("Intentional error on close");
    }

    @Override
    public synchronized boolean next(LongWritable key, ErrorWritable value)
    throws IOException {
      if(err.equals(Error.NEXT)) {
	if(errNum < errMax) {
	  ++errNum;
	  super.next(key, value);
	  throw new IOException("Intentional error on next");
	}
      }
      return super.next(key, value);
    }

  }


}