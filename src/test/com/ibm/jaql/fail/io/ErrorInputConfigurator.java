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

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.hadoop.FileInputConfigurator;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class ErrorInputConfigurator extends FileInputConfigurator {

  public static JsonString ERROR_NAME = new JsonString("error");
  public static JsonString ERROR_MAX_NAME = new JsonString("errorMax");

  private ErrorInputFormat.Error err = ErrorInputFormat.Error.NONE;
  private int errorMax = Integer.MAX_VALUE;

  @Override
  public void init(JsonValue options) throws Exception {
    super.init(options);
    JsonString code = (JsonString)AdapterStore.getStore().input.getOption((JsonRecord)options).get(ERROR_NAME);
    err = ErrorInputFormat.Error.valueOf(code.toString());
    JsonNumber max = (JsonNumber)AdapterStore.getStore().input.getOption((JsonRecord)options).get(ERROR_MAX_NAME);
    if(max != null) {
      errorMax = max.intValue();
    }
  }

  @Override
  protected void set(JobConf conf) throws Exception {
    // TODO Auto-generated method stub
    super.set(conf);
    conf.set(ErrorInputFormat.ERROR_NAME, err.toString());
    conf.setInt(ErrorInputFormat.ERROR_NEXT_MAX, errorMax);
  }

}