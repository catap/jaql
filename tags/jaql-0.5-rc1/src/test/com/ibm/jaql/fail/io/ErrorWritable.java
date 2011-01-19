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

import org.apache.hadoop.io.Text;

public class ErrorWritable extends Text {

  public enum Error {	NONE,READ,WRITE };

  private Error error = Error.NONE;

  public ErrorWritable() { }

  public ErrorWritable(Error e) {
    error = e;
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    String e = in.readUTF();
    error = Error.valueOf(e);
    super.readFields(in);

    if(error.equals(Error.READ)) {
      throw new IOException("failed in read");
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(error.toString());
    super.write(out);

    if(error.equals(Error.WRITE)) {
      throw new IOException("failed in write");
    }
  }

}