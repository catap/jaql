/*
 * Copyright (C) IBM Corp. 2010.
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

import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;


/** Converts a LongWriteable to a JsonLong */
public class LongWritableToJson implements ToJson<LongWritable>
{
  @Override
  public JsonLong convert(LongWritable src, JsonValue target)
  {
    MutableJsonLong result = (target instanceof MutableJsonLong) ? (MutableJsonLong) target : new MutableJsonLong();
    result.set( src.get() );
    return result;
  }

  @Override
  public MutableJsonLong createTarget()
  {
    return new MutableJsonLong();
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.longSchema();
  }

}
