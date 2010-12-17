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
package com.ibm.jaql.io.serialization.text.schema;

import java.io.IOException;
import java.io.InputStream;

import com.ibm.jaql.io.serialization.SerializerMap;
import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.io.serialization.text.TextFullSerializer;
import com.ibm.jaql.io.serialization.text.def.DefaultTextFullSerializer;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.FastPrinter;

/**
 * Jaql's default serializer for the shell. Uses schema information, e.g., to determine order of 
 * fields in a record. 
 */
public final class SchemaTextFullSerializer extends TextFullSerializer {
  /** holds the schema associated with this serializer */
  private Schema schema;
  
  /** Describes the basic serializers */
  private MySerializerMap serializers;
  
  /** serializer used for "nonnull" and "null" schemata */  
  private DefaultTextFullSerializer defaultSerializer = DefaultTextFullSerializer.getInstance();

  // -- construction -----------------------------------------------------------

  public SchemaTextFullSerializer(Schema schema) {
    this.schema = SchemaTransformation.compact(schema);
    serializers = new MySerializerMap(this.schema);
  }

  // -- FullSerializer methods -------------------------------------------------

  @Override
  public JsonValue read(InputStream in, JsonValue target) throws IOException {
    try {
      JsonParser parser = new JsonParser(in);
      return parser.JsonVal(); // does not check schema
    } catch (ParseException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(FastPrinter out, JsonValue value, int indent) throws IOException {
    // handle nulls
    if (value == null)
    {
      if (serializers.matchesNull())
      {
        defaultSerializer.write(out, value, indent);
        return;
      }
      throw new IllegalArgumentException("null value is not matched by this serializer");
    }

    // find a serializer
    SerializerInfo k = serializers.get(value);
    if (k == null)
    {
      // none found
      if (serializers.matchesNonNull()) 
      {
        defaultSerializer.write(out, value, indent);
        return;
      }
      throw new IllegalArgumentException("value " + value + " does not match this serializer's " +
          "schema " + schema);
    }
    else
    {
      k.serializer.write(out, value, indent);
    }
  }

  
  public static TextBasicSerializer<?> makeBasicSerializer(Schema schema)
  {
    switch (schema.getSchemaType()) {
    case RECORD:
      return new RecordSerializer((RecordSchema)schema);
    case ARRAY:
      return new ArraySerializer((ArraySchema)schema);
    default:
      return new DefaultBasicSerializer(schema);
    }
  }


  // -- helper classes ----------------------------------------------------------------------------
  
  /** Describes the information associated with serializers */
  private static class SerializerInfo 
  {
    final Schema schema;
    @SuppressWarnings("unchecked")
    final TextBasicSerializer serializer;
    
    public SerializerInfo(Schema schema, TextBasicSerializer<?> serializer)
    {
      this.schema = schema;
      this.serializer = serializer;
    }
  }
  
  /** Implementation of SerializerMap for the above SerializerInfo class */
  private class MySerializerMap extends SerializerMap<SerializerInfo>
  {
    public MySerializerMap(Schema schema)
    {
      super(schema);
    }
    
    @Override
    public SerializerInfo makeSerializerInfo(int pos, Schema schema)
    {
      return new SerializerInfo(schema, makeBasicSerializer(schema));
    }

    @Override
    public Schema schemaOf(SerializerInfo info)
    {
      return info.schema;
    }    
  }
  
  /** Used for schemata that do not have special text formatting. Redirects to the default 
   * text serializers. */
  private static class DefaultBasicSerializer extends TextBasicSerializer<JsonValue>
  {
    final Schema schema;
    private DefaultTextFullSerializer defaultSerializer = DefaultTextFullSerializer.getInstance();
    
    public DefaultBasicSerializer(Schema schema)
    {
      this.schema = schema;
    }
    
    @Override
    public void write(FastPrinter out, JsonValue value) throws IOException
    {
      write(out, value, 0);      
    }

    @Override
    public void write(FastPrinter out, JsonValue value, int indent)
        throws IOException
    {
      if (!schema.matchesUnsafe(value))
      {
        throw new IllegalArgumentException("value not matched by this serializer");
      }
      defaultSerializer.write(out, value, indent);
    }
  }


  
}
