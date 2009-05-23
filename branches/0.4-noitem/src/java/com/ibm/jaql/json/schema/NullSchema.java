package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonValue;

public class NullSchema extends Schema
{
  private static final NullSchema THE_INSTANCE = new NullSchema();
  
  public static NullSchema getInstance()
  {
    return THE_INSTANCE;
  }
  
  private NullSchema()
  {    
  }
  
  
  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    return value == null;
  }
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.NULL;
  }
}
