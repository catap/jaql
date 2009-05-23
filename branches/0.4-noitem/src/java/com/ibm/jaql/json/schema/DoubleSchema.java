package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonValue;

public class DoubleSchema extends NumericSchema<JsonDouble>
{
  // TODO: precision

  public DoubleSchema()
  {
    super();
  }
  
  public DoubleSchema(JsonDouble min, JsonDouble max)
  {
    super(min, max);
  }

  public DoubleSchema(JsonNumeric min, JsonNumeric max)
  {
    this(convert(min), convert(max));
  }
  
  private static JsonDouble convert(JsonNumeric v)
  {
    if (v == null || v instanceof JsonDouble)
    {
      return (JsonDouble)v;
    }
    throw new IllegalArgumentException("interval argument has to be of type double: " + v);
  }

  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonDouble))
    {
      return false;
    }

    // value is double, as are min and max
    if ( (min != null && value.compareTo(min)<0) || (max != null && value.compareTo(max)>0) )
    {
      return false;
    }
    
    return true;
  }
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.DOUBLE;
  }
}
