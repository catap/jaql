/**
 * 
 */
package com.ibm.jaql.json.type;

import java.util.HashMap;

import com.ibm.jaql.lang.expr.function.Function;

/** Enumeration of all JSON types. */
public enum JsonType
{
  UNKNOWN(null, ""), // bogus item type used as an indicator
  // UNDEFINED(null, null), // reserved for possible inclusion of the undefined value
  NULL(null, "null"), 
  ARRAY(JsonArray.class, "array"), 
  RECORD(JsonRecord.class, "record"),
  BOOLEAN(JsonBool.class, "boolean"),
  STRING(JsonString.class, "string"),
  LONG(JsonLong.class, "long"),
  DOUBLE(JsonDouble.class, "double"),
  DECFLOAT(JsonDecimal.class, "decfloat"),

  // JSON extensions

  BINARY(JsonBinary.class, "binary"),
  DATE(JsonDate.class, "date"),
  SCHEMA(JsonSchema.class, "schematype"),
  FUNCTION(Function.class, "function"),

  // Extensiblity for writable java objects, but the class name is written on every instance!

  JAVAOBJECT(JsonJavaObject.class, "javaObject"), // extend by any writable object

  // Experimental types - They might disappear!

  REGEX(JsonRegex.class, "regex"), SPAN(JsonSpan.class, "span");

  private static final HashMap<String, JsonType>  STRING_TO_TYPE = new HashMap<String, JsonType>();
  private static final HashMap<JsonString, JsonType> NAME_TO_TYPE = new HashMap<JsonString, JsonType>();
  private static final HashMap<Class<? extends JsonValue>, JsonType> CLASS_TO_TYPE = new HashMap<Class<? extends JsonValue>, JsonType>();

  static
  {
    for (JsonType t : values())
    {
      STRING_TO_TYPE.put(t.name.toString(), t);
      NAME_TO_TYPE.put(t.name, t);
      CLASS_TO_TYPE.put(t.mainClass, t);
    }
  }

  private final Class<? extends JsonValue> mainClass;
  private final JsonString                 name;
  private final boolean                    isNumber;

  JsonType(Class<? extends JsonValue> mainClass, String name)
  {
    this.mainClass = mainClass;
    this.name = new JsonString(name);
    this.isNumber = mainClass!=null && JsonNumber.class.isAssignableFrom(mainClass); 
  }
  
  public static JsonType getType(String name)
  {
    return STRING_TO_TYPE.get(name);
  }

  public static JsonType getType(JsonString name)
  {
    return NAME_TO_TYPE.get(name);
  }
  
  @SuppressWarnings("unchecked")
  public static JsonType getType(Class<? extends JsonValue> clazz)
  {
    JsonType t = CLASS_TO_TYPE.get(clazz); 
    if (t == null)
    {
      // check superclass
      t = getType((Class<? extends JsonValue>)clazz.getSuperclass());
      if (t != null)
      {
        CLASS_TO_TYPE.put(clazz, t);
      }
    }
    return t;
  }
  
  /**
   * @param x
   * @param y
   * @return
   */
  public static int typeCompare(JsonValue x, JsonValue y)
  {
    if (x == null)
    {
      return y == null ? 0 : -1;
    }
    if (y == null)
    {
      return 1;
    }
    return x.getType().compareTo(y.getType());
  }
  
  public JsonString getName()
  {
    return name;
  }

  public String toString()
  {
    return name.toString();
  }
  
  public Class<? extends JsonValue> getMainClass()
  {
    return mainClass;
  }
  
  public boolean isNumber()
  {
    return isNumber;
  }
}