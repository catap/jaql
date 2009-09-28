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
package com.ibm.jaql.io.serialization.binary.temp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.schema.NonNullSchema;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.SchematypeSchema;
import com.ibm.jaql.json.schema.BinarySchema;
import com.ibm.jaql.json.schema.BooleanSchema;
import com.ibm.jaql.json.schema.DateSchema;
import com.ibm.jaql.json.schema.DecfloatSchema;
import com.ibm.jaql.json.schema.DoubleSchema;
import com.ibm.jaql.json.schema.GenericSchema;
import com.ibm.jaql.json.schema.LongSchema;
import com.ibm.jaql.json.schema.NullSchema;
import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.schema.StringSchema;
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

/** 
 * Jaql's serializer for temporary files. Mainly used for serialization in between the map and
 * reduce phase. The file format is not stable. 
 *
 * This serializer uses an efficient serialization format if schema information is given. This works
 * as follows. 
 * 
 * The provided schema is first broken into basic schemata, i.e., schemata that 
 * represent a basic type but not <code>schema null</code> or <code>schema any</code>. For example, 
 * <code>schema any | long | null | string</code> is broken into the parts 
 * <code>schema long</code> and <code>schema string</code>. For each of the two parts, a basic 
 * serializer is instantiated: <code>LongSerializer</code> and <code>StringSerializer</code>, 
 * in this case. 
 *
 * When a value is written that matches one of the basic schemata, the serializer first writes encoding
 * <code>INDEX_OFFSET+index</code>, where <code>index</code> is a unique index associated with
 * each basic schema. It then uses the associated basic serializer to serialize the value. 
 * 
 * When a value is written that does not match any of the basic schemata, but does match 
 * <code>schema null</code> or <code>schema any</code> (if present), then the value is directly 
 * written using the {@link DefaultBinaryFullSerializer}. This serializer will first write an
 * encoding strictly less than <code>INDEX_OFFSET</code>, followed by the value.
 * 
 * When a value is read, the serializer first reads the encoding from the input stream. If it is
 * greater than or equal to <code>INDEX_OFFSET</code>, a basic schema is matched and the respective 
 * basic serializer is used for reading. Otherwise, the default serializer associated with
 * the encoding just read is used to read the value.
 * 
 * If this class matches only one schema (a single basic schema or null or any), then no type 
 * information is written.
 */
public final class TempBinaryFullSerializer extends BinaryFullSerializer // for the moment
{
  // -- private variables -------------------------------------------------------------------------
  
  /** serializer used for "any" and "null" schemata */  
  private DefaultBinaryFullSerializer defaultSerializer = DefaultBinaryFullSerializer.getInstance();
  
  /** all values below INDEX_OFFSET are assumed to be reserved for the encodings of the default 
   * serializer that is used when no schema information is given. */
  private final int INDEX_OFFSET = 64;
  
  /** holds the schema associated with this serializer */
  private Schema schema;
  
  /** true if there is only a single basic serializer associated with this serializer */
  boolean hasSingleSerializer;
  
  /** true if the schema matches "null" */
  boolean matchesNull;
  
  /** true if the schema matches "any" */
  boolean matchesAny;
  
  /** Maps subclasses of JsonValue to a list of possible serializers for this subclass */ 
  private Map<Class<? extends JsonValue>, List<SerializerInfo>> serializerMap;
  
  /** A list of serializers (ordered by their encoding) */
  private List<SerializerInfo> serializerList;
   
  /** Describes the information associated with serializers */
  private class SerializerInfo
  {
    final int encoding;
    final Schema schema;
    @SuppressWarnings("unchecked")
    final BinaryBasicSerializer serializer;
    
    SerializerInfo(int index, Schema schema, BinaryBasicSerializer<?> serializer)
    {
      this.encoding = INDEX_OFFSET + index;
      this.schema = schema;
      this.serializer = serializer;
    }
  }
  
  // -- construction ------------------------------------------------------------------------------
  
  public TempBinaryFullSerializer(Schema schema) {
    assert JsonEncoding.LIMIT < INDEX_OFFSET; // these values are used for null/any
    // remove duplicates and make sure each type occurs at most once
    this.schema = SchemaTransformation.compact(schema);
    init();
  }
  
  /** Initializes the internal data structures (in particular: serializerMap and serializerList) */
  private void init()
  {
    // create empty data structures
    serializerMap = new HashMap<Class<? extends JsonValue>,  List<SerializerInfo>>();
    serializerList = new ArrayList<SerializerInfo>();
    
    // any and null are treated separately
    matchesAny = false;
    matchesNull = false;
    hasSingleSerializer = false;
    
    // expand OrSchema
    if (schema instanceof OrSchema)
    {
      for (Schema s : ((OrSchema)schema).get())
      {
        add(s); // s is not an OrSchema because OrSchemas are not nested
      }
    }
    else 
    {
      hasSingleSerializer = true;
      add(schema);
    }
  }
  
  /** Adds the given schema to the internal data structures */
  private void add(Schema schema)
  {
    assert !(schema instanceof OrSchema);
    if (schema instanceof NullSchema)
    {
      matchesNull = true;
    } 
    else if (schema instanceof NonNullSchema)
    {
      matchesAny = true;
    }
    else
    {
      // add serializer to list
      SerializerInfo entry = new SerializerInfo(serializerList.size(), schema, makeBasicSerializer(schema));
      serializerList.add(entry);
      
      // and to map
      for (Class<? extends JsonValue> clazz : schema.matchedClasses())
      {
        List<SerializerInfo> list = serializerMap.get(clazz);
        if (list == null)
        {
          list = new ArrayList<SerializerInfo>();
        }
        list.add(entry);
        serializerMap.put(clazz, list);
      }
    }    
  }

  // -- full serialization ------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  @Override
  public JsonValue read(DataInput in, JsonValue target) throws IOException
  {
    if (hasSingleSerializer)
    {
      // there is just one serializer
      if (matchesNull)
      {
        // matches null only
        return null;
      }
      else if (matchesAny)
      {
        // matches any only
        return defaultSerializer.read(in, target);
      }
      else
      {
        // matches some other schema --> retrieve and read
        return serializerList.get(0).serializer.read(in, target);
      }
    }
    else
    {
      // there are multiple serializers --> get the encoding
      int encoding = BaseUtil.readVUInt(in);
      BinaryBasicSerializer<?> serializer = getSerializer(encoding);
      return serializer.read(in, target); 
    }   
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(DataOutput out, JsonValue value) throws IOException
  {
    // handle nulls
    if (value == null)
    {
      if (matchesNull)
      {
        if (!hasSingleSerializer) 
        {
          // will write null encoding < INDEX_OFFSET
          defaultSerializer.write(out, value);
        }
        else
        {
          // matches null only --> write nothing (common for map keys!)
        }
        return;
      }
      throw new IllegalArgumentException("null value is not matched by this serializer");
    }
    
    // find a serializer
    SerializerInfo k = getSerializer(value);
    if (k == null)
    {
      // none found
      if (matchesAny) 
      {
        // will write encoding < INDEX_OFFSET
        defaultSerializer.write(out, value);
        return;
      }
      throw new IllegalArgumentException("value " + value + " does not match this serializer's " +
      		"schema " + schema);
    }
    else
    {
      // at least one found
      if (!hasSingleSerializer)
      {
        // will write encoding >= INDEX_OFFSET
        BaseUtil.writeVUInt(out, k.encoding);
      }
      k.serializer.write(out, value);
    }
  }

  
  // -- basic serializers -------------------------------------------------------------------------

  /** Creates a basic serializer for the specified schema. Used at initialization time. */
  public static BinaryBasicSerializer<?> makeBasicSerializer(Schema schema)
  {
    switch (schema.getSchemaType())
    {
    case BOOLEAN:
      return new BooleanSerializer((BooleanSchema)schema);
    case LONG:
      return new LongSerializer((LongSchema)schema);
    case DECFLOAT:
      return new DecfloatSerializer((DecfloatSchema)schema);
    case DOUBLE:
      return new DoubleSerializer((DoubleSchema)schema);
    case STRING:
      return new StringSerializer((StringSchema)schema);
    case BINARY:
      return new BinarySerializer((BinarySchema)schema);
    case DATE:
      return new DateSerializer((DateSchema)schema);
    case ARRAY:
      return new ArraySerializer((ArraySchema)schema);
    case RECORD:
      return new RecordSerializer((RecordSchema)schema);
    case SCHEMATYPE:
      return new SchemaSerializer((SchematypeSchema)schema);
    case GENERIC:
      // "function" and "aschema" falls into this category
      // currently, there is only one encoding for those types; checked below
      // DefaultBinaryFullSerializer is used to write them
      JsonType type = ((GenericSchema)schema).getType();
      JsonEncoding encoding = null;
      for (JsonEncoding e : JsonEncoding.values())
      {
        if (e.getType().equals(type)) 
        {
          if (encoding == null)
          {
            encoding = e;
          }
          else
          {
            throw new IllegalStateException("generic schemata are requried to have types with only one encoding");
          }
        }
      }
      assert encoding != null; // every type has an encoding
      return DefaultBinaryFullSerializer.getInstance().getSerializer(encoding);
    case NULL:            // handled separately
    case NON_NULL:    // handled separately
    case OR:              // has been unrolled
      throw new IllegalArgumentException("implementation error");
    default:
      throw new IllegalArgumentException("unsupported schema type");
    }
  }
  
  /** Returns a basic serializer for the given non-null value. It is not guaranteed that the schema 
   * associated with returned serializer matches <code>value</code> but it is guaranteed that 
   * if there is a schema that matches <code>value</code>, it is returned. Used for writing. */  
  SerializerInfo getSerializer(JsonValue value)
  {
    assert value != null;
    
    // non-null values
    List<SerializerInfo> list = serializerMap.get(value.getClass());
    if (list == null)
    {
      // search for super classes
      Class<?> superClass = value.getClass().getSuperclass();
      while (superClass != null)
      {
        list = serializerMap.get(superClass);
        if (list != null)
        {
          serializerMap.put(value.getClass(), list);
          break;
        }
        superClass = superClass.getSuperclass();
      }
      if (list == null) return null; // failed
    }
    
    int n = list.size();
    // n-1 to avoid expensive matches(...) computation in the common case where there is 
    // just one serializer
    for (int i=0; i<n-1; i++) 
    {
      SerializerInfo s = list.get(i);
      try
      {
        if (s.schema.matches(value)) { 
          return s;
        }
      } catch (Exception e)
      {
      }
    }
    SerializerInfo result = list.get(n-1);
    if (matchesAny && !result.schema.matchesUnsafe(value)) // saves matches(...) check when possible 
    {
      return null;
    }
    return result;
  }
  
  /** Returns the basic serializer that belongs to the specified encoding */
  private BinaryBasicSerializer<?> getSerializer(int encoding)
  {
    if (encoding < INDEX_OFFSET)
    {
      // encoded using default serializer
      return defaultSerializer.getSerializer(JsonEncoding.getEncoding(encoding)); 
    }
    else
    {
      // encoded using one of our serializers
      return serializerList.get(encoding-INDEX_OFFSET).serializer;
    }
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  @SuppressWarnings("unchecked")
  @Override
  public int compare(DataInput in1, DataInput in2) throws IOException 
  {
    if (hasSingleSerializer)
    {
      // there is just one serializer
      if (matchesNull)
      {
        // matches null only
        return 0;
      }
      else if (matchesAny)
      {
        // matches any only
        return defaultSerializer.compare(in1, in2);
      }
      else
      {
        // matches some other schema --> retrieve and read
        return serializerList.get(0).serializer.compare(in1, in2);
      }
    }
    else
    {
      // there are multiple serializers --> get the encoding
      int encoding1 = BaseUtil.readVUInt(in1);
      int encoding2 = BaseUtil.readVUInt(in2);
      
      if (encoding1 == encoding2)
      {
        BinaryBasicSerializer<?> serializer = getSerializer(encoding1);
        return serializer.compare(in1, in2);
      }
      else
      {
        // try to determine JSON types
        boolean default1 = encoding1<INDEX_OFFSET;
        boolean default2 = encoding2<INDEX_OFFSET;
        JsonType type1;
        if (default1)
        {
          type1 = JsonEncoding.getEncoding(encoding1).getType();
        }
        else
        {
          type1 = serializerList.get(encoding1-INDEX_OFFSET).schema.getSchemaType().getJsonType();
        }
        JsonType type2;
        if (default2)
        {
          type2 = JsonEncoding.getEncoding(encoding2).getType();
        }
        else
        {
          type2 = serializerList.get(encoding2-INDEX_OFFSET).schema.getSchemaType().getJsonType();
        }

        // compare the types
        if (type1 != null && type2 != null)
        {
          int cmp = type1.compareTo(type2);
          if (cmp != 0) return cmp;
        }
        
        // otherwise: give up and deserialize
        BinaryBasicSerializer<?> serializer1 = getSerializer(encoding1);
        BinaryBasicSerializer<?> serializer2 = getSerializer(encoding2);
        JsonValue value1 = serializer1.read(in1, null);
        JsonValue value2 = serializer2.read(in2, null);
        return JsonUtil.compare(value1, value2);
      }
    }   
  }
}
