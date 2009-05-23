package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonValue;

public class DecimalSchema extends NumericSchema<JsonDecimal>
{
  // TODO: precision

  public DecimalSchema()
  {
    super();
  }
  
  public DecimalSchema(JsonDecimal min, JsonDecimal max)
  {
    super(min, max);
  }
  
  public DecimalSchema(JsonNumeric min, JsonNumeric max)
  {
    this(convert(min), convert(max));
  }
  
  private static JsonDecimal convert(JsonNumeric v)
  {
    if (v == null || v instanceof JsonDecimal)
    {
      return (JsonDecimal)v;
    }
    if (v instanceof JsonLong)
    {
      return new JsonDecimal(v.longValue());
    }
    throw new IllegalArgumentException("interval argument has to be of type long or decimal: " + v);
  }
  
  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonLong || value instanceof JsonDecimal))
    {
      return false;
    }

    // value can be long or decimal, min and max are decimal
    if ( (min != null && min.compareTo(value)>0) || (max != null && max.compareTo(value)<0) )
    {
      return false;
    }
    
    return true;
  }
  

  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.DECIMAL;
  }
}
