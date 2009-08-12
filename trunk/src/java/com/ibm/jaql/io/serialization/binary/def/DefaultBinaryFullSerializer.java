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
package com.ibm.jaql.io.serialization.binary.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumMap;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.util.BaseUtil;

/** Jaql's default serializer. This serializer is generic; it does not consider/exploit any
 * schema information. */
public class DefaultBinaryFullSerializer extends BinaryFullSerializer
{
  final EnumMap<JsonEncoding, BinaryBasicSerializer<?>> serializers; 

  // caches
  final JsonValue[] atoms1 = new JsonValue[JsonEncoding.LIMIT];
  final JsonValue[] atoms2 = new JsonValue[JsonEncoding.LIMIT];

  
  // -- default instance --------------------------------------------------------------------------
  
  private static DefaultBinaryFullSerializer defaultInstance = new DefaultBinaryFullSerializer();
  public static DefaultBinaryFullSerializer getInstance() {
    if (defaultInstance == null) { 
      // TODO: code block needed; why is defaultInstance not initialized?
      // once reslove, make defaultInstance final      
      defaultInstance = new DefaultBinaryFullSerializer();
    }
    return defaultInstance;
  }
  
  
  // -- construction ------------------------------------------------------------------------------

  public DefaultBinaryFullSerializer() {
    assert JsonEncoding.LIMIT == 20; // change when adding the encodings
    
    serializers = new EnumMap<JsonEncoding, BinaryBasicSerializer<?>>(JsonEncoding.class);
    
    BinaryBasicSerializer<JsonString> jstringSerializer = new JsonStringSerializer();
    
//    UNKNOWN(0, null, Type.UNKNOWN), // bogus item type used as an indicator
//    UNDEFINED(1, null, null), // reserved for possible inclusion of the undefined value
    serializers.put(JsonEncoding.NULL, new NullSerializer());
    serializers.put(JsonEncoding.ARRAY_SPILLED, new SpilledJsonArraySerializer(this));
    serializers.put(JsonEncoding.ARRAY_BUFFERED, new BufferedJsonArraySerializer(this));
    serializers.put(JsonEncoding.RECORD, new BufferedJsonRecordSerializer(
        jstringSerializer, this));
    serializers.put(JsonEncoding.BOOLEAN, new JsonBoolSerializer());
    serializers.put(JsonEncoding.STRING, jstringSerializer);
    serializers.put(JsonEncoding.BINARY, new JsonBinarySerializer());
    serializers.put(JsonEncoding.LONG, new JsonLongSerializer());
    serializers.put(JsonEncoding.DECFLOAT, new JsonDecimalSerializer());
    serializers.put(JsonEncoding.DATE, new JsonDateSerializer());
    serializers.put(JsonEncoding.FUNCTION, new JaqlFunctionSerializer());
    serializers.put(JsonEncoding.SCHEMA, new JsonSchemaSerializer());
    serializers.put(JsonEncoding.JAVAOBJECT_CLASSNAME, new JsonJavaObjectSerializer());
    serializers.put(JsonEncoding.REGEX, new JsonRegexSerializer(jstringSerializer));
    serializers.put(JsonEncoding.SPAN, new JsonSpanSerializer());
    serializers.put(JsonEncoding.DOUBLE, new JsonDoubleSerializer());
//    JAVA_RECORD(18, JavaJRecord.class, Type.RECORD),
    serializers.put(JsonEncoding.JAVA_ARRAY, new JavaJsonArraySerializer());
  }

  
  // -- FullSerializer methods --------------------------------------------------------------------

  @Override
  public JsonValue read(DataInput in, JsonValue target) throws IOException
  {
    int encodingId = BaseUtil.readVUInt(in);
    JsonEncoding encoding = JsonEncoding.getEncoding(encodingId);
    BinaryBasicSerializer<?> serializer = serializers.get(encoding);
    return serializer.read(in, target);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(DataOutput out, JsonValue value) throws IOException
  {
    JsonEncoding encoding;
    if (value == null) {
      encoding = JsonEncoding.NULL;
    } else {
      encoding = value.getEncoding();
    }
    BaseUtil.writeVUInt(out, encoding.getId());
    BinaryBasicSerializer serializer = serializers.get(encoding);
    assert serializer != null : "No serializer defined for " + encoding;
    serializer.write(out, value);    
  }

  @Override
  public void skip(DataInput in) throws IOException {
    int encodingId = BaseUtil.readVUInt(in);
    JsonEncoding encoding = JsonEncoding.getEncoding(encodingId);
    BinaryBasicSerializer<?> serializer = serializers.get(encoding);
    serializer.skip(in);
  }

  @SuppressWarnings("unchecked")
  @Override
  public int compare(DataInput in1, DataInput in2) throws IOException {
    // read and compare encodings / types
    int code1 = BaseUtil.readVUInt(in1);
    int code2 = BaseUtil.readVUInt(in2);
    assert code1>0 && code2>0;
    JsonEncoding encoding1 = JsonEncoding.getEncoding(code1);
    JsonEncoding encoding2 = JsonEncoding.getEncoding(code2);
    if (encoding1 != encoding2) {
      JsonType type1 = encoding1.getType();
      JsonType type2 = encoding2.getType();
      int cmp = type1.compareTo(type2);
      if (cmp != 0) return cmp;

      // if same type but different encodings, deserialize
      // TODO: a better way / treat some cases special?
      return compareValuesDeserialized(in1, encoding1, in2, encoding2);
    }
    
    // same encoding
    BinaryBasicSerializer s = getSerializer(encoding1);
    return s.compare(in1, in2);
  }
  
  @Override
  public void copy(DataInput in, DataOutput out) throws IOException {
    int encodingId = BaseUtil.readVUInt(in);
    JsonEncoding encoding = JsonEncoding.getEncoding(encodingId);
    BinaryBasicSerializer<?> serializer = serializers.get(encoding);
    
    BaseUtil.writeVUInt(out, encodingId);
    serializer.copy(in, out);
  }
  
  
  // -- misc --------------------------------------------------------------------------------------

  /** Returns the <code>BasicSerializer</code> used for the given <code>encoding</code>. */
  public BinaryBasicSerializer<?> getSerializer(JsonEncoding encoding) {
    BinaryBasicSerializer<?> serializer = serializers.get(encoding);
    assert serializer != null : "No serializer defined for " + encoding;
    return serializer;
  }
  

  /** Compares the encoded value from <code>in1</code> with the encoded value from 
   * <code>in2</code> by deserializing. This method is used to compare types of different
   * encodings.   
   * 
   * @param in1 an input stream pointing to a value
   * @param in2 another input stream pointing to another value with the same encoding
   * @return
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  protected int compareValuesDeserialized(DataInput in1, JsonEncoding encoding1, 
      DataInput in2, JsonEncoding encoding2) throws IOException 
  {
    BinaryBasicSerializer s1 = getSerializer(encoding1);
    BinaryBasicSerializer s2 = getSerializer(encoding2);

    // atoms can be overwritten; they are only used here 
    JsonValue value1 = atoms1[encoding1.getId()] = s1.read(in1, atoms1[encoding1.getId()]);
    JsonValue value2 = atoms2[encoding2.getId()] = s2.read(in2, atoms2[encoding2.getId()]);
    return value1.compareTo(value2);
  }
  
}
