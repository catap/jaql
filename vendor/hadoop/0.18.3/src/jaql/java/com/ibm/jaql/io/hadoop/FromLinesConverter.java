/*
 * Copyright (C) IBM Corp. 2008.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.ibm.jaql.io.hadoop.converter.KeyValueImport;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;

/** Converts a text file into an array of lines. */
public class FromLinesConverter implements KeyValueImport<LongWritable, Text> {
  
  // -- constants ---------------------------------------------------------------------------------
  
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(FromLinesConverter.class.getName());
  public static final JsonString FORMAT = new JsonString("org.apache.hadoop.mapred.TextInputFormat");
  public static final JsonString CONVERTER_NAME = new JsonString(FromLinesConverter.class.getName());;

  // -- constants ---------------------------------------------------------------------------------
  
  /** Initializes this converter. */
  @Override
  public void init(JsonRecord options)
  {
  }

  /** Creates a fresh target. */
  @Override
  public JsonValue createTarget()
  {
    return new MutableJsonString();
  }
  
  /** Converts the given line into a JSON value. */
  @Override
  public JsonValue convert(LongWritable key, Text value, JsonValue target)
  {
    System.out.print(key + ":" + value.toString() + ": ");
    ((MutableJsonString)target).set(value.getBytes(), value.getLength());
    System.out.println(target);
    return target;
  }      
      
  @Override
  public Schema getSchema()
  {
    return SchemaFactory.stringSchema();
  }
}