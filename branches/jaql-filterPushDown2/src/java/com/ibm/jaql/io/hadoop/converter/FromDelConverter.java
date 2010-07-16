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
package com.ibm.jaql.io.hadoop.converter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.ibm.jaql.io.converter.AbstractFromDelConverter;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

/** Converts a delimited file into JSON. See {@link AbstractFromDelConverter}. */
public final class FromDelConverter extends AbstractFromDelConverter<LongWritable, Text>
{
  public static final JsonString FORMAT = new JsonString("org.apache.hadoop.mapred.TextInputFormat");
  public static final JsonString CONVERTER_NAME = new JsonString(FromDelConverter.class.getName());;


  /** Converts the given line into a JSON value. */
  @Override
  public JsonValue convert(LongWritable key, Text value, JsonValue target)
  {
    return convert(key.get(), value.getBytes(), value.getLength(), target); 
  }
}