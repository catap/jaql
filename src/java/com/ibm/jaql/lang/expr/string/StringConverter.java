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
package com.ibm.jaql.lang.expr.string;

import static com.ibm.jaql.json.type.JsonType.ARRAY;
import static com.ibm.jaql.json.type.JsonType.BOOLEAN;
import static com.ibm.jaql.json.type.JsonType.DATE;
import static com.ibm.jaql.json.type.JsonType.DECFLOAT;
import static com.ibm.jaql.json.type.JsonType.DOUBLE;
import static com.ibm.jaql.json.type.JsonType.LONG;
import static com.ibm.jaql.json.type.JsonType.NULL;
import static com.ibm.jaql.json.type.JsonType.RECORD;
import static com.ibm.jaql.json.type.JsonType.STRING;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonDate;
import com.ibm.jaql.json.type.MutableJsonDecimal;
import com.ibm.jaql.json.type.MutableJsonDouble;
import com.ibm.jaql.json.type.MutableJsonLong;

/** Converts an input value (string, array of strings or record with string values) to 
 * the specified types. */
public class StringConverter
{
//-- variables ---------------------------------------------------------------------------------
  
  /** Holder for various informations about desired output */
  private static class Descriptor
  {
    JsonType type;
    boolean isNullable;
    int fieldIndex;
  }
 
  Schema schema = null;
  Descriptor main;
  boolean mainIsAtomic;
  JsonValue emptyTarget;
  Descriptor[] sub;
  Map<JsonString, Descriptor> fieldMap;
 

  // -- construction ------------------------------------------------------------------------------

  /**
   * @param exprs
   */
  public StringConverter(Schema schema)
  {
    init(schema);
  }

 
  // -- initialization ----------------------------------------------------------------------------

  /** Parse and validate the input schema and populate the internal data structures for efficient
   * conversion. */
  private void init(Schema schema)
  {
    if (schema.hasModifiers())
    {
      throw new IllegalArgumentException("type modifiers are currently not allowed for conversion");
    }
   
    this.schema = schema;
   
    // check whether we are given an atomic type
    main = new Descriptor();
    mainIsAtomic = true;
    if (initAtomic(schema, main))
    {
      return;
    }
   
    // we are not atomic
    mainIsAtomic = false;
    schema = SchemaTransformation.removeNullability(schema); // will never produce null
   
    // check whether we are given an array
    if (schema instanceof ArraySchema)
    {
      ArraySchema arraySchema = (ArraySchema)schema;
      if (arraySchema.hasRest())
      {
        throw new IllegalArgumentException("invalid input schema");
      }
      List<Schema> schemata = arraySchema.getHeadSchemata();
      int n = schemata.size();
      sub = new Descriptor[n];
      BufferedJsonArray a = new BufferedJsonArray(n);
      for (int i=0; i<n; i++)
      {
        sub[i] = new Descriptor();
        if (!initAtomic(schemata.get(i), sub[i]))
        {
          throw new IllegalArgumentException("invalid input schema");
        }
        a.set(i, emptyTarget);
      }
      main.type = ARRAY;
      emptyTarget = a;
      return;
    }

    // check for a record
    if (schema instanceof RecordSchema)
    {
      RecordSchema recordSchema = (RecordSchema)schema;
      if (recordSchema.hasAdditional())
      {
        throw new IllegalArgumentException("invalid input schema");
      }
      List<RecordSchema.Field> fields = recordSchema.getFields();
      int n = fields.size();
      sub = new Descriptor[n];
      fieldMap = new HashMap<JsonString, Descriptor>();
      BufferedJsonRecord r = new BufferedJsonRecord(n);
      for (int i=0; i<n; i++)
      {
        sub[i] = new Descriptor();
        r.add(fields.get(i).getName(), null);
        fieldMap.put(fields.get(i).getName(), sub[i]);
      }
      r.sort();
     
      for (int i=0; i<n; i++)
      {
       
        RecordSchema.Field field = fields.get(i);
        if (field.isOptional())
        {
          throw new IllegalArgumentException("optional fields are not supported");
        }
        if (!initAtomic(field.getSchema(), sub[i]))
        {
          throw new IllegalArgumentException("invalid input schema");
        }
        sub[i].fieldIndex = r.indexOf(field.getName());
        r.set(sub[i].fieldIndex, emptyTarget);
      }

      main.type = RECORD;
      emptyTarget = r;
      return;
    }
   
    throw new IllegalArgumentException("invalid input schema");
  }
 
 
  /** Initializes the given descriptor for the given schema. Returns false is the input
   * schema is an array or record. Throws an exception if the input schema is unsupported. */
  boolean initAtomic(Schema schema, Descriptor out)
  {
    out.isNullable = schema.is(NULL).maybe();
    schema = SchemaTransformation.removeNullability(schema);
    if (schema == null)
    {
      out.type = NULL;
      return true;
    }
   
    switch (schema.getSchemaType())
    {
    case BOOLEAN:
      out.type = BOOLEAN;
      emptyTarget = null; // unused
      break;
    case LONG:
      out.type = LONG;
      emptyTarget = new MutableJsonLong();
      break;
    case DOUBLE:
      out.type = DOUBLE;
      emptyTarget = new MutableJsonDouble();
      break;
    case DECFLOAT:
      out.type = DECFLOAT;
      emptyTarget = new MutableJsonDecimal();
      break;
    case STRING:
      out.type = STRING;
      emptyTarget = null; // unused
      break;
    case DATE:
      out.type = DATE;
      emptyTarget = new MutableJsonDate();
      break;
    case ARRAY:
    case RECORD:
      return false;
    default:
      throw new IllegalArgumentException("invalid input schema");
    }
    return true;
  }
 


  // -- conversion --------------------------------------------------------------------------------

  private JsonValue convertBasic(JsonString in, Descriptor out, JsonValue target)
  {
    assert out != null;
    assert in != null;
   
    switch (out.type)
    {
    case NULL:
      throw new RuntimeException("null expected");
    case BOOLEAN:
      return JsonBool.make(in);
    case LONG:
      if (target == null)
      {
        target = new MutableJsonLong(JsonLong.parseLong(in));
      }
      else
      {
        ((MutableJsonLong)target).set(JsonLong.parseLong(in));
      }
      return target;
    case DOUBLE:
      if (target == null)
      {
        target = new MutableJsonDouble(JsonDouble.parseDouble(in));
      }
      else
      {
        ((MutableJsonDouble)target).set(JsonDouble.parseDouble(in));
      }
      return target;
    case DECFLOAT:
      if (target == null)
      {
        target = new MutableJsonDecimal(JsonDecimal.parseDecimal(in));
      }
      else
      {
        ((MutableJsonDecimal)target).set(JsonDecimal.parseDecimal(in));
      }
      return target;
    case STRING:
      return in;      
    case DATE:
      if (target == null)
      {
        target = new MutableJsonDate(in.toString());
      }
      else
      {
        ((MutableJsonDate)target).set(in.toString());
      }
      return target;
    default:
      throw new IllegalArgumentException("type conversion to " + out.type.getName() + " unsupported.");
    }
  }
 
  private JsonValue convertArray(JsonArray in, JsonValue targetValue)
  {
    if (in.count() != sub.length)
    {
      throw new RuntimeException("input array has invalid length");
    }
   
    BufferedJsonArray target = (BufferedJsonArray)targetValue;
    int i=0;
    for (JsonValue value : in)
    {
      if (value == null)
      {
        if (sub[i].isNullable)
        {
          target.set(i, null);
        }
        else
        {
          throw new RuntimeException("found null value in field " + i + ", expected " + sub[i].type);
        }
      }
      else
      {
        target.set(i, convertBasic((JsonString)value, sub[i], target.get(i)));
      }
      ++i;
    }    
    return target;
  }
 
  private JsonValue convertRecord(JsonRecord in, JsonValue targetValue)
  {
    if (in.size() != sub.length)
    {
      throw new RuntimeException("input record has invalid number of fields");
    }
   
    BufferedJsonRecord target = (BufferedJsonRecord)targetValue;
    int i=0;
    for (Entry<JsonString, JsonValue> entry : in)
    {
      Descriptor d = fieldMap.get(entry.getKey());
      if (d == null)
      {
        throw new RuntimeException("input record has invalid field" + entry.getKey());
      }
      JsonValue value = entry.getValue();
      if (value == null)
      {
        if (d.isNullable)
        {
          target.set(d.fieldIndex, null);
        }
        else
        {
          throw new RuntimeException("found null value in field " + i + ", expected " + d.type);
        }
      }
      else
      {
        target.set(d.fieldIndex, convertBasic((JsonString)value, d, target.get(i)));
      }
      ++i;
    }    
    return target;
  }
 
 
  // -- evaluation --------------------------------------------------------------------------------
 
  public JsonValue createTarget()
  {
    return JsonUtil.getCopyUnchecked(emptyTarget, null);
  }
 
  /*
   * (non-Javadoc)
   *
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue convert(JsonValue value, JsonValue target)
  {
    if (value == null)
    {
      if (main.isNullable)
      {
        return null;
      }
      else
      {
        throw new RuntimeException("found null value, expected " + main.type);
      }
    }
    if (mainIsAtomic)
    {
      return convertBasic((JsonString)value, main, target);      
    }
    else if (main.type == RECORD)
    {
      return convertRecord((JsonRecord)value, target);
    }
    else if (main.type == ARRAY)
    {
      return convertArray((JsonArray)value, target);
    }

    throw new IllegalStateException("should not happen");
  }
 
  public Schema getSchema()
  {
    return schema;
  }
  
  /**
   * Return true if the entire object being converted by the present instance of
   * the converter is nullable.
   * 
   * @return true if nullable, or false otherwise.
   */
  public boolean isNullable() {
    return main.isNullable;
  }
  
  /**
   * Return true if the objects converted by the string converted are atomic. 
   * @return
   */
  public boolean isAtomic() {
    return mainIsAtomic;
  }
  
  /**
   * Return the expected type of the main object as parsed from the schema.
   * @return
   */
  public JsonType getType() {
    return main.type;
  }
}
