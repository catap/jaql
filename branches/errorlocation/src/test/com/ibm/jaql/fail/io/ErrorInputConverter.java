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

import com.ibm.jaql.io.hadoop.converter.KeyValueImport;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;

public class ErrorInputConverter implements KeyValueImport<LongWritable, ErrorWritable> {

  public final static JsonString ERROR_NAME = new JsonString("convError");
  public final static JsonString ERROR_MAX_NAME = new JsonString("convMax");

  private JsonBool err = JsonBool.FALSE;
  private int errMax = Integer.MAX_VALUE;
  private int errNum = 0;

  @Override
  public JsonValue convert(LongWritable key, ErrorWritable val,
      JsonValue target) {
    if(err.get()) {
      if(errNum < errMax) {
	++errNum;
	throw new RuntimeException("Intentional converter error");
      }
    }
    MutableJsonString conv = (MutableJsonString)target;
    if(conv == null) {
      conv = (MutableJsonString)createTarget();
    }
    conv.set(val.getBytes());
    return conv;
  }

  @Override
  public JsonValue createTarget() {
    return new MutableJsonString();
  }

  @Override
  public Schema getSchema() {
    return SchemaFactory.stringOrNullSchema();
  }

  @Override
  public void init(JsonRecord options) {
    JsonBool e = (JsonBool)options.get(ERROR_NAME);
    if(e != null) 
      err = e;
    JsonNumber max = (JsonNumber)options.get(ERROR_MAX_NAME);
    if(max != null)
      errMax = max.intValue();
  }

}