package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonType;

/** Enumeration of all schema types. Each schema is implemented by exactly one class. */
public enum SchemaType implements Comparable<SchemaType>
{
  // determines ordering
  
  // atoms
  BOOLEAN(JsonType.BOOLEAN),
  LONG(JsonType.LONG), DOUBLE(JsonType.DOUBLE), DECFLOAT(JsonType.DECFLOAT), 
  STRING(JsonType.STRING), BINARY(JsonType.BINARY),
  DATE(JsonType.DATE), FUNCTION(JsonType.FUNCTION),
  SCHEMATYPE(JsonType.SCHEMA),
  
  // compound types
  ARRAY(JsonType.ARRAY), RECORD(JsonType.RECORD), 

  OR(null),

  // null
  NON_NULL(null), 
  NULL(JsonType.NULL), 

  // container for the rest
  GENERIC(null);
  
  private JsonType type;
  
  SchemaType(JsonType type)
  {
    this.type = type;
  }
  
  /** Returns a JSON type of null if there is no single type that is matched. */
  public JsonType getJsonType()
  {
    return type;
  }
}