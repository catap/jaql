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
package com.ibm.jaql.io.serialization.text.basic;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.EnumMap;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.io.serialization.text.TextFullSerializer;
import com.ibm.jaql.io.serialization.text.def.BoolSerializer;
import com.ibm.jaql.io.serialization.text.def.DecimalSerializer;
import com.ibm.jaql.io.serialization.text.def.DoubleSerializer;
import com.ibm.jaql.io.serialization.text.def.LongSerializer;
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonValue;

/**
 * A serializer that converts basic types to literals and the rest become
 * strings with json inside. It uses {@link JsonAsStringSerializer} to serialize
 * JSON values which are not of type JSON boolean, JSON long, JSON decfloat and
 * JSON double.
 */
public final class BasicTextFullSerializer extends TextFullSerializer
{
  private final EnumMap<JsonEncoding, TextBasicSerializer<?>> serializers; 
  private final JsonAsStringSerializer stringSerializer = new JsonAsStringSerializer();


  // -- default instance -------------------------------------------------------
  
  private static final BasicTextFullSerializer defaultInstance = new BasicTextFullSerializer();

  public static BasicTextFullSerializer getInstance() 
  {
      return defaultInstance;
  }
  
  
  // -- construction -----------------------------------------------------------

  public BasicTextFullSerializer()
  {
    serializers = new EnumMap<JsonEncoding, TextBasicSerializer<?>>(JsonEncoding.class);
    serializers.put(JsonEncoding.BOOLEAN,  new BoolSerializer());
    serializers.put(JsonEncoding.LONG,     new LongSerializer());
    serializers.put(JsonEncoding.DECFLOAT, new DecimalSerializer());
    serializers.put(JsonEncoding.DOUBLE,   new DoubleSerializer());
  }

  
  // -- FullSerializer methods -------------------------------------------------

  @Override
  public JsonValue read(InputStream in, JsonValue target) throws IOException
  {
    throw new UnsupportedOperationException("basic reading is not yet supported");
  }

  
  @Override
  public void write(PrintStream out, JsonValue value, int indent) throws IOException
  {
    write(out, value, indent, true);
  }
  
  @SuppressWarnings("unchecked")
  public void write(PrintStream out, JsonValue value, int indent, boolean escape) throws IOException
  {
    // null values are simply not printed
    if( value != null )
    {
      JsonEncoding encoding = value.getEncoding();
      TextBasicSerializer serializer = serializers.get(encoding);
      if( serializer == null )
      {
        stringSerializer.write(out, value, indent, escape);
      }
      else 
      {
        serializer.write(out, value, indent);  
      }
    }
  }
}
