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
package com.ibm.jaql.io.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.ibm.jaql.io.hadoop.converter.KeyValueImport;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.string.StringConverter;

/** 
 * Converts a text file into an array of lines. 
 * If schema is provided as an option, then convert it to the appropriate
 * types specified by the schema.
 */
public class FromLinesConverter implements KeyValueImport<LongWritable, Text> {
  
  // -- constants ---------------------------------------------------------------------------------
  private static final Log LOG = LogFactory.getLog(FromLinesConverter.class);
  public static final JsonString CONVERT_NAME = new JsonString("convert");

  // -- constants ---------------------------------------------------------------------------------
  
  private Schema schema;
  private StringConverter converter;
  
  /** Initializes this converter. */
  @Override
  public void init(JsonRecord options) {
    if (options == null) {
      LOG.warn("No options passed, using the default options.");
      options = JsonRecord.EMPTY;
    }
    
    // Check for a converter.
    schema = SchemaFactory.stringSchema();
    JsonValue arg = options.get(CONVERT_NAME);
    if (arg != null) {
      schema = ((JsonSchema)arg).get();
      if (!schema.is(JsonType.BOOLEAN, JsonType.DATE, JsonType.DECFLOAT, 
          JsonType.DOUBLE, JsonType.LONG, JsonType.NULL, JsonType.STRING).always()) {
        throw new IllegalArgumentException("lines() is for atomic types, use del() for complex types");
      }
    }
    converter = new StringConverter(schema);
  }

  /** Creates a fresh target. */
  @Override
  public JsonValue createTarget() {
    return converter.createTarget();
  }
  
  /** Converts the given line into a JSON value. */
  @Override
  public JsonValue convert(LongWritable key, Text value, JsonValue target) {
    target = converter.convert(new JsonString(value.toString()), target);
    return target;
  }
      
  @Override
  public Schema getSchema() {
    return schema;
  }
}