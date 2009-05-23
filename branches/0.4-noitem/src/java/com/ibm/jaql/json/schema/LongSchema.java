package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonValue;

public class LongSchema extends NumericSchema<JsonLong>
{
  // TODO: precision

  public LongSchema()
  {
    super();
  }
  
  public LongSchema(JsonLong min, JsonLong max)
  {
    super(min, max);
  }
  
  public LongSchema(JsonNumeric min, JsonNumeric max)
  {
    this(convert(min), convert(max));
  }
  
  private static JsonLong convert(JsonNumeric v)
  {
    if (v == null || v instanceof JsonLong)
    {
      return (JsonLong)v;
    }
    if (v instanceof JsonDecimal)
    {
      JsonDecimal d = (JsonDecimal)v;
      try 
      {
        return new JsonLong(d.longValueExact());
      }
      catch (ArithmeticException e)
      {
        throw new IllegalArgumentException("interval argument too large or fractional: " + d);
      }
    }
    throw new IllegalArgumentException("interval argument has to be of type long: " + v);
  }
  
  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonLong || value instanceof JsonDecimal))
    {
      return false;
    }

    // value can be long or decimal, min and max are long
    if ( (min != null && value.compareTo(min)<0) || (max != null && value.compareTo(max)>0) )
    {
      return false;
    }
    
    return true;
  }
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.LONG;
  }
}
