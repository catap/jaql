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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;

public class ErrorOutputFormat extends SequenceFileOutputFormat<LongWritable, ErrorWritable> {

  public static String ERROR_NAME = "ErrorOutputFormat.code";
  public static String ERROR_NEXT_MAX = "ErrorOutputFormat.errorMax";

  public enum Error { NONE, CONFIG, OPEN, NEXT, CLOSE };

  @Override
  public RecordWriter<LongWritable, ErrorWritable> getRecordWriter(
      FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
      throws IOException {
    String val = arg1.get(ERROR_NAME);
    Error e = Error.valueOf(val);
    // if OPEN, fail
    if(e.equals(Error.OPEN)) {
      throw new IOException("Intentional error on open");
    }
    int max = arg1.getInt(ERROR_NEXT_MAX, 1);
    // Wrap in an ErrorRecordWriter
    return new ErrorRecordWriter(super.getRecordWriter(arg0, arg1, arg2, arg3), e, max);
  }

  @Override
  public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
  throws FileAlreadyExistsException, InvalidJobConfException,
  IOException {
    String val = arg1.get(ERROR_NAME);
    Error e = Error.valueOf(val);
    // if CONFIG, fail
    if(e.equals(Error.CONFIG)) {
      throw new IOException("Intentional error on config");
    }
    super.checkOutputSpecs(arg0, arg1);
  }

  public class ErrorRecordWriter<LongWritable, ErrorWritable> implements RecordWriter<LongWritable, ErrorWritable> {

    RecordWriter<LongWritable, ErrorWritable> rw;
    Error err;
    int errMax;
    int errNum = 0;
    

    public ErrorRecordWriter(RecordWriter<LongWritable, ErrorWritable> r, Error e, int max) {
      rw = r;
      err = e;
      errMax = max;
    }

    @Override
    public void close(Reporter arg0) throws IOException {
      if(err.equals(Error.CLOSE)) {
	throw new IOException("Intentional error on close");
      }
      rw.close(arg0);
    }

    @Override
    public void write(LongWritable arg0, ErrorWritable arg1)
    throws IOException {
      if(err.equals(Error.NEXT)) {
	if(errNum < errMax) {
	  ++errNum;
	  throw new IOException("Intentional error on next");
	}	
      }
      rw.write(arg0, arg1);
    }

  }
}