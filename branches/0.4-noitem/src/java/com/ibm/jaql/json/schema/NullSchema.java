package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.Bool3;

/** Schema for the null value */
public class NullSchema extends Schema
{
  // -- singleton ---------------------------------------------------------------------------------

  private static NullSchema theInstance = null;
  
  public static NullSchema getInstance()
  {
    if (theInstance == null)
    {
      theInstance = new NullSchema();
    }
    return theInstance;
  }
  
  private NullSchema()
  {    
  }

  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.NULL;
  }

  @Override
  public Bool3 isNull()
  {
    return Bool3.TRUE;
  }

  @Override
  public Bool3 isConst()
  {
    return Bool3.TRUE;
  }

  @Override
  public Bool3 isArray()
  {
    return Bool3.FALSE;
  }

  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    return value == null;
  }
}
