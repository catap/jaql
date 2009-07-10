/**
 * 
 */
package com.ibm.jaql.json.type;

import java.util.HashMap;

import com.ibm.jaql.lang.core.JaqlFunction;

/** Enumeration of all JSON encodings. */
public enum JsonEncoding
{
  // UNKNOWN(0, null, Type.UNKNOWN), // bogus item type used as an indicator
  // UNDEFINED(1, null, null), // reserved for possible inclusion of the undefined value
  NULL(2, null, JsonType.NULL),
  ARRAY_SPILLING(3, SpilledJsonArray.class, JsonType.ARRAY),
  ARRAY_FIXED(4, BufferedJsonArray.class, JsonType.ARRAY),
  MEMORY_RECORD(5, BufferedJsonRecord.class, JsonType.RECORD),
  BOOLEAN(6, JsonBool.class, JsonType.BOOLEAN),
  STRING(7, JsonString.class, JsonType.STRING),
  BINARY(8, JsonBinary.class, JsonType.BINARY),
  LONG(9, JsonLong.class, JsonType.NUMBER),
  DECIMAL(10, JsonDecimal.class, JsonType.NUMBER),
  DATE_MSEC(11, JsonDate.class, JsonType.DATE),
  FUNCTION(12, JaqlFunction.class, JsonType.FUNCTION),
  SCHEMA(13, JsonSchema.class, JsonType.SCHEMA),
  JAVAOBJECT_CLASSNAME(14, JsonJavaObject.class, JsonType.JAVAOBJECT), // extension type that lists class name next
  REGEX(15, JsonRegex.class, JsonType.REGEX),
  SPAN(16, JsonSpan.class, JsonType.SPAN),
  DOUBLE(17, JsonDouble.class, JsonType.DOUBLE),
  JAVA_RECORD(18, JavaJsonRecord.class, JsonType.RECORD), 
  JAVA_ARRAY(19, JavaJsonRecord.class, JsonType.ARRAY);

  public final static int                                        LIMIT        = 20;                                             // keep at max id + 1
  private static final JsonEncoding[]                                idToEncoding = new JsonEncoding[LIMIT];
  private static final HashMap<Class<? extends JsonValue>, Integer> classMap     = new HashMap<Class<? extends JsonValue>, Integer>();

  public final int                                               id;
  public final Class<? extends JsonValue>                           clazz;
  public final JsonType                                              type;

  static
  {
    for (JsonEncoding e : values())
    {
      idToEncoding[e.id] = e;
      classMap.put(e.clazz, e.id);
    }
  }

  JsonEncoding(int id, Class<? extends JsonValue> clazz, JsonType type)
  {
    assert type != null;
    this.id = id;
    this.clazz = clazz;
    this.type = type;
    // classMap.put(clazz, this);
  }

  public static JsonEncoding getEncoding(int id)
  {
    return idToEncoding[id];
  }

  public JsonValue newInstance()
  {
    try
    {
      return (JsonValue) clazz.newInstance();
    }
    catch (InstantiationException e)
    {
      throw new RuntimeException(clazz.getName(), e);
    }
    catch (IllegalAccessException e)
    {
      throw new RuntimeException(clazz.getName(), e);
    }
  }

  public JsonType getType()
  {
    return type;
  }
}