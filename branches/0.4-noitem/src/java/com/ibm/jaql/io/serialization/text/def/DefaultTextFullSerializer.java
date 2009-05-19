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

/** Jaql's default serializer. This serializer is generic; it does not consider/exploit any
 * schema information. */
public final class DefaultTextFullSerializer extends TextFullSerializer
{
  final EnumMap<JsonEncoding, TextBasicSerializer<?>> serializers; 

  // caches
  final JsonValue[] atoms1 = new JsonValue[JsonEncoding.LIMIT];
  final JsonValue[] atoms2 = new JsonValue[JsonEncoding.LIMIT];

  
  // -- default instance --------------------------------------------------------------------------
  
  private static DefaultTextFullSerializer defaultInstance = new DefaultTextFullSerializer();
  public static DefaultTextFullSerializer getInstance() {
    if (defaultInstance == null) { 
      // TODO: code block needed; why is defaultInstance not initialized?
      // once reslove, make defaultInstance final      
      defaultInstance = new DefaultTextFullSerializer();
    }
    return defaultInstance;
  }
  
  
  // -- construction ------------------------------------------------------------------------------

  public DefaultTextFullSerializer() {
    assert JsonEncoding.LIMIT == 20; // change when adding the encodings
    
    serializers = new EnumMap<JsonEncoding, TextBasicSerializer<?>>(JsonEncoding.class);
    
    TextBasicSerializer<JsonString> jstringSerializer = new JsonStringSerializer();
    
//    UNKNOWN(0, null, Type.UNKNOWN), // bogus item type used as an indicator
//    UNDEFINED(1, null, null), // reserved for possible inclusion of the undefined value
    serializers.put(JsonEncoding.NULL, new NullSerializer());
    serializers.put(JsonEncoding.ARRAY_SPILLING, new JsonArraySerializer(this));
    serializers.put(JsonEncoding.ARRAY_FIXED, new JsonArraySerializer(this));
    serializers.put(JsonEncoding.MEMORY_RECORD, new JsonRecordSerializer(
        jstringSerializer, this));
    serializers.put(JsonEncoding.BOOLEAN, new JsonBoolSerializer());
    serializers.put(JsonEncoding.STRING, jstringSerializer);
    serializers.put(JsonEncoding.BINARY, new JsonBinarySerializer());
    serializers.put(JsonEncoding.LONG, new JsonLongSerializer());
    serializers.put(JsonEncoding.DECIMAL, new JsonDecimalSerializer());
    serializers.put(JsonEncoding.DATE_MSEC, new JsonDateSerializer());
    serializers.put(JsonEncoding.FUNCTION, new JaqlFunctionSerializer());
    serializers.put(JsonEncoding.SCHEMA, new JsonSchemaSerializer());
//    serializers.put(JsonEncoding.JAVAOBJECT_CLASSNAME, new JsonJavaObjectSerializer());
    serializers.put(JsonEncoding.REGEX, new JsonRegexSerializer(jstringSerializer));
    serializers.put(JsonEncoding.SPAN, new JsonSpanSerializer());
    serializers.put(JsonEncoding.DOUBLE, new JsonDoubleSerializer());
//    JAVA_RECORD(18, JavaJRecord.class, Type.RECORD),
//    serializers.put(JsonEncoding.JAVA_ARRAY, new JavaJsonArraySerializer());
  }

  
  // -- FullSerializer methods --------------------------------------------------------------------

  @Override
  public JsonValue read(InputStream in, JsonValue target) throws IOException
  {
    try {
      JsonParser parser = new JsonParser(in);
      return parser.JsonVal();
    } catch (ParseException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(PrintStream out, JsonValue value, int indent) throws IOException
  {
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

  
  // -- misc --------------------------------------------------------------------------------------

  /** Returns the <code>BasicSerializer</code> used for the given <code>encoding</code>. */
  public TextBasicSerializer<?> getSerializer(JsonEncoding encoding) {
    TextBasicSerializer<?> serializer = serializers.get(encoding);
    assert serializer != null : "No serializer defined for " + encoding;
    return serializer;
  }

  
}
