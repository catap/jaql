/**
 * 
 */
package com.ibm.jaql.json.type;

import java.util.HashMap;

import com.ibm.jaql.lang.core.JaqlFunction;

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
  NUMBER(JsonNumber.class, "number"),

  // JSON extensions

  BINARY(JsonBinary.class, "binary"),
  DATE(JsonDate.class, "date"),
  SCHEMA(JsonSchema.class, "aschema"),
  FUNCTION(JaqlFunction.class, "function"),

  // Extensiblity for writable java objects, but the class name is written on every instance!

  JAVAOBJECT(JsonJavaObject.class, "javaObject"), // extend by any writable object

  // Experimental types - They might disappear!

  REGEX(JsonRegex.class, "regex"), SPAN(JsonSpan.class, "span"), DOUBLE(
      JsonDouble.class, "double");

  private static final HashMap<String, JsonType>  nameToType  = new HashMap<String, JsonType>();
  private static final HashMap<JsonString, JsonType> jnameToType = new HashMap<JsonString, JsonType>();
  private static final HashMap<Class<? extends JsonValue>, JsonType> classToType = new HashMap<Class<? extends JsonValue>, JsonType>();

  static
  {
    for (JsonType t : values())
    {
      nameToType.put(t.name, t);
      jnameToType.put(t.nameValue, t);
      classToType.put(t.clazz, t);
    }
  }

  public final Class<? extends JsonValue>        clazz;
  public final String                         name;
  public final JsonString                        nameValue;
  //public final Item                           nameItem;

  JsonType(Class<? extends JsonValue> clazz, String name)
  {
    this.clazz = clazz;
    this.name = name;
    this.nameValue = new JsonString(name);
    // BUG: this is a circular dependency, i.e., Item -> Encoding -> Type -> Item
    //this.nameItem = new Item(nameValue);
  }

  public static JsonType getType(String name)
  {
    return nameToType.get(name);
  }

  public static JsonType getType(JsonString name)
  {
    return jnameToType.get(name);
  }
  
  public static JsonType getType(Class<? extends JsonValue> clazz)
  {
    return classToType.get(clazz);
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
    return x.getEncoding().type.compareTo(y.getEncoding().type);
  }
}