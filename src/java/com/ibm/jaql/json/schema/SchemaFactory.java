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
package com.ibm.jaql.json.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;

/** Constructs schemata for commonly used situations */
public class SchemaFactory
{
  
  /** Tries to construct the tightest possible schema that matches the given value */
  public static Schema schemaOf(JsonValue v)
  {
    if (v == null)
    {
      return NullSchema.getInstance();
    }
    
    JsonLong length;
    switch (v.getEncoding().getType())
    {
    case ARRAY: 
      List<Schema> schemata = new ArrayList<Schema>();
      for (JsonValue vv : (JsonArray)v)
      {
        schemata.add(schemaOf(vv));
      }
      return new ArraySchema(schemata.toArray(new Schema[schemata.size()]));
      
    case RECORD:
      List<RecordSchema.Field> fields = new ArrayList<RecordSchema.Field>();
      for (Entry<JsonString, JsonValue> e : (JsonRecord)v)
      {
        JsonString name = e.getKey();
        JsonValue value = e.getValue();
        fields.add(new RecordSchema.Field(name, schemaOf(value), true));
      }
      return new RecordSchema(fields.toArray(new RecordSchema.Field[fields.size()]), null);
      
    case BOOLEAN:
      return new BooleanSchema((JsonBool)v);
      
    case STRING:
      JsonString js = (JsonString)v;
      return new StringSchema(null, null, null, js);
      
    case NUMBER:
      switch (v.getEncoding())
      {
      case DECIMAL:
        return new DecimalSchema(null, null, (JsonDecimal)v);
      case LONG:
        return new LongSchema(null, null, (JsonLong)v);
      default:
        throw new IllegalStateException(); // only reached when new number encodings are added
      }

    case DOUBLE:
      return new DoubleSchema(null, null, (JsonDouble)v);
      
    // JSON extensions

    case BINARY:
      JsonBinary jb = (JsonBinary)v;
      length = new JsonLong(jb.getLength());
      return new BinarySchema(length, length);
      
    case DATE:
      JsonDate jd = (JsonDate)v;
      return new DateSchema(null, null, jd);
      
    case SCHEMA:
      return new GenericSchema(JsonType.SCHEMA);
    
    case FUNCTION:
      return new GenericSchema(JsonType.FUNCTION);
      
    case JAVAOBJECT:
    case REGEX:
    default:
      return AnySchema.getInstance();
    }
  }
}
