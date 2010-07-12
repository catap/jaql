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
import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.hadoop.FileOutputConfigurator;
import com.ibm.jaql.io.hadoop.HadoopSerializationDefault;
import com.ibm.jaql.io.hadoop.JsonHolderDefault;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class ErrorOutputConfigurator extends FileOutputConfigurator {

  public static JsonString ERROR_NAME = new JsonString("error");
  public static JsonString ERROR_MAX_NAME = new JsonString("errorMax");

  private ErrorOutputFormat.Error err = ErrorOutputFormat.Error.NONE;
  private int errMax = 1;

  @Override
  public void init(JsonValue options) throws Exception {
    // TODO Auto-generated method stub
    super.init(options);
    JsonString code = (JsonString)AdapterStore.getStore().output.getOption((JsonRecord)options).get(ERROR_NAME);
    err = ErrorOutputFormat.Error.valueOf(code.toString());
    JsonNumber max = (JsonNumber)AdapterStore.getStore().output.getOption((JsonRecord)options).get(ERROR_MAX_NAME);
    if(max != null) errMax = max.intValue();
  }

  @Override
  public void setParallel(JobConf conf) throws Exception {
    // TODO Auto-generated method stub
    super.setParallel(conf);
    conf.set(ErrorOutputFormat.ERROR_NAME, err.toString());
    conf.setInt(ErrorOutputFormat.ERROR_NEXT_MAX, errMax);
  }

  @Override
  public void setSequential(JobConf conf) throws Exception {
    // TODO Auto-generated method stub
    super.setSequential(conf);
    conf.set(ErrorOutputFormat.ERROR_NAME, err.toString());
    conf.setInt(ErrorOutputFormat.ERROR_NEXT_MAX, errMax);
  }

  @Override
  protected void registerSerializers(JobConf conf) {
    conf.setMapOutputKeyClass(JsonHolderDefault.class);
    conf.setMapOutputValueClass(JsonHolderDefault.class);
    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(ErrorWritable.class);
    HadoopSerializationDefault.register(conf);
  }

}