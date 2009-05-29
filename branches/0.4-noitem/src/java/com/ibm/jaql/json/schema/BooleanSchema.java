package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;
import com.ibm.jaql.util.Bool3;

/** Schema for a boolean value */
public class BooleanSchema extends Schema 
{
  private JsonBool value;
  
  // -- schema parameters -------------------------------------------------------------------------
  
  private static Parameters parameters = null; 
  
  public static Parameters getParameters()
  {
    if (parameters == null)
    {
      parameters = new Parameters(
          new JsonString[] { PAR_VALUE           },
          new Schema[]     { new BooleanSchema() },
          new JsonValue[]  { null                });
    }
    return parameters;
  }

  
  // -- construction ------------------------------------------------------------------------------
  
  public BooleanSchema(JsonRecord args)
  {
    this(
        (JsonBool)getParameters().argumentOrDefault(PAR_VALUE, args));
  }
  
  public BooleanSchema(JsonBool value)
  {
    this.value = value;
  }
  
  public BooleanSchema()
  {
  }
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.BOOLEAN;
  }

  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  @Override
  public Bool3 isConst()
  {
    return value == null ? Bool3.UNKNOWN : Bool3.TRUE;
  }

  @Override
  public Bool3 isArray()
  {
    return Bool3.FALSE;
  }
  
  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    if (!(value instanceof JsonBool))
    {
      return false;
    }
    JsonBool b = (JsonBool)value;
    
    if (this.value != null)
    {
      return b.equals(this.value);
    }
    return true;
  }
  

  // -- getters -----------------------------------------------------------------------------------
  
  public JsonBool getValue()
  {
    return value;
  }
}
