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

import org.apache.hadoop.io.Text;

import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;

/** Convert a hadoop Text object to a JsonString */
public class TextToJsonString implements ToJson<Text> 
{

  @Override
  public MutableJsonString createTarget()
  {
    return new MutableJsonString();
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.stringSchema();
  }
  
  @Override
  public JsonValue convert(Text src, JsonValue target)
  {
    MutableJsonString str;
    if( target instanceof MutableJsonString )
    {
      str = (MutableJsonString)target;
    }
    else
    {
      str = new MutableJsonString();
    }
    str.set(src.getBytes(), src.getLength());
    return str;
  }
}
