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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import com.ibm.jaql.fail.io.ErrorInputFormat.Error;

public class ErrorSplit implements InputSplit
{
  public Error error;
  public int count = 1;
  public InputSplit child;

  public ErrorSplit() { }

  public ErrorSplit(Path file, long start, long length, JobConf conf, Error e, int cnt) throws IOException {
    child = new FileSplit(file, start, length, conf);
    error = e;
    count = cnt;
  }

  public ErrorSplit(Path file, long start, long length, String[] hosts, Error e, int cnt) throws IOException {
    child = new FileSplit(file, start, length, hosts);
    error = e;
    count = cnt;
  }

  public ErrorSplit(FileSplit split, JobConf job, Error e, int cnt) throws IOException {
    child = new FileSplit(split.getPath(), split.getStart(), split.getLength(), split.getLocations());
    error = e;
    count = cnt;
  }

  @Override
  public long getLength() throws IOException {
    return child.getLength();
  }

  @Override
  public String[] getLocations() throws IOException {
    return child.getLocations();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    error = Error.valueOf(in.readUTF());
    count = in.readInt();
    try
    {
      Class<?> c = Class.forName("org.apache.hadoop.mapred.FileSplit").asSubclass(InputSplit.class);;
      this.child = (InputSplit) ReflectionUtils.newInstance(c, null);
      this.child.readFields(in);
    }
    catch (ClassNotFoundException ce)
    {
      throw new IOException(ce.getMessage());
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(error.toString());
    out.writeInt(count);
    child.write(out);
  }

}