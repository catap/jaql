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

import org.apache.hadoop.io.LongWritable;

import com.ibm.jaql.io.converter.FromJson;
import com.ibm.jaql.io.hadoop.converter.JsonToHadoopRecord;
import com.ibm.jaql.io.hadoop.converter.KeyValueExport;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class ErrorOutputConverter implements KeyValueExport<LongWritable, ErrorWritable> {

  public final static JsonString CONV_ERROR = new JsonString("convError");
  public final static JsonString CONV_ERROR_MAX = new JsonString("convMax");
  public final static JsonString WRITE_ERROR = new JsonString("writeError");

  private ErrorWritable.Error writeErr = ErrorWritable.Error.NONE;	
  private JsonBool err = JsonBool.FALSE;
  private int errMax = 1;
  private int errNum = 0;

  @Override
  public void convert(JsonValue src, LongWritable key, ErrorWritable val) {

    if(err.get()) {
      if(errNum < errMax) {
	++errNum;
	throw new RuntimeException("Intentional converter error");
      }
    }
    val.set(src.toString());
  }

  @Override
  public LongWritable createKeyTarget() {
    return new LongWritable();
  }

  @Override
  public ErrorWritable createValueTarget() {
    return new ErrorWritable(writeErr);
  }

  @Override
  public void init(JsonRecord options) {
    JsonBool e = (JsonBool) options.get(CONV_ERROR);
    if(e != null) {
      err = e;
    }
    JsonNumber max = (JsonNumber) options.get(CONV_ERROR_MAX);
    if(max != null) {
      errMax = max.intValue();
    }
    JsonString w = (JsonString) options.get(WRITE_ERROR);
    if(w != null) {
      writeErr = ErrorWritable.Error.valueOf(w.toString());
    }
  }

}