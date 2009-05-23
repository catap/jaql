package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.util.JaqlUtil;

public class GenericSchema extends Schema
{
  JsonType type;
  
  public GenericSchema(JsonType type)
  {
    JaqlUtil.enforceNonNull(type);
    this.type = type;
  }

  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    return type.clazz.isInstance(value);
  }

  public JsonType getType()
  {
    return type;  
  }
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.GENERIC;
  }
}
