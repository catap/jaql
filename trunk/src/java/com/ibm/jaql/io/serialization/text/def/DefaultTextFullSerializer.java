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
package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.EnumMap;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.io.serialization.text.TextFullSerializer;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

/**
 * Jaql's default serializer. This serializer is generic; it does not
 * consider/exploit any schema information.
 */
public final class DefaultTextFullSerializer extends TextFullSerializer {
  private final EnumMap<JsonEncoding, TextBasicSerializer<?>> serializers;

  // -- default instance -------------------------------------------------------

  private static DefaultTextFullSerializer defaultInstance = new DefaultTextFullSerializer();

  public static DefaultTextFullSerializer getInstance() {
    if (defaultInstance == null) {
      // TODO: code block needed; why is defaultInstance not initialized?
      // once reslove, make defaultInstance final
      defaultInstance = new DefaultTextFullSerializer();
    }
    return defaultInstance;
  }

  // -- construction -----------------------------------------------------------

  public DefaultTextFullSerializer() {
    assert JsonEncoding.LIMIT == 20; // change when adding the encodings

    serializers = new EnumMap<JsonEncoding, TextBasicSerializer<?>>(JsonEncoding.class);

    TextBasicSerializer<JsonString> jstringSerializer = new StringSerializer();

    // UNKNOWN(0, null, Type.UNKNOWN), // bogus item type used as an indicator
    // UNDEFINED(1, null, null), // reserved for possible inclusion of the undefined value
    serializers.put(JsonEncoding.NULL, new NullSerializer());
    serializers.put(JsonEncoding.ARRAY_SPILLED, new ArraySerializer(this));
    serializers.put(JsonEncoding.ARRAY_BUFFERED, new ArraySerializer(this));
    serializers.put(JsonEncoding.RECORD,
                    new RecordSerializer(jstringSerializer, this));
    serializers.put(JsonEncoding.BOOLEAN, new BoolSerializer());
    serializers.put(JsonEncoding.STRING, jstringSerializer);
    serializers.put(JsonEncoding.BINARY, new BinarySerializer());
    serializers.put(JsonEncoding.LONG, new LongSerializer());
    serializers.put(JsonEncoding.DECFLOAT, new DecimalSerializer());
    serializers.put(JsonEncoding.DATE, new DateSerializer());
    serializers.put(JsonEncoding.FUNCTION, new FunctionSerializer());
    serializers.put(JsonEncoding.SCHEMA, new SchemaSerializer());
    serializers.put(JsonEncoding.REGEX,
                    new RegexSerializer(jstringSerializer));
    serializers.put(JsonEncoding.SPAN, new SpanSerializer());
    serializers.put(JsonEncoding.DOUBLE, new DoubleSerializer());
  }

  // -- FullSerializer methods -------------------------------------------------

  @Override
  public JsonValue read(InputStream in, JsonValue target) throws IOException {
    try {
      JsonParser parser = new JsonParser(in);
      return parser.JsonVal();
    } catch (ParseException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(PrintStream out, JsonValue value, int indent) throws IOException {
    JsonEncoding encoding;
    if (value == null) {
      encoding = JsonEncoding.NULL;
    } else {
      encoding = value.getEncoding();
    }
    TextBasicSerializer serializer = serializers.get(encoding);
    assert serializer != null : "No serializer defined for " + encoding;
    serializer.write(out, value, indent);
  }

  // -- misc -------------------------------------------------------------------

  /**
   * Returns the <code>BasicSerializer</code> used for the given
   * <code>encoding</code>.
   */
  public TextBasicSerializer<?> getSerializer(JsonEncoding encoding) {
    TextBasicSerializer<?> serializer = serializers.get(encoding);
    assert serializer != null : "No serializer defined for " + encoding;
    return serializer;
  }

}
