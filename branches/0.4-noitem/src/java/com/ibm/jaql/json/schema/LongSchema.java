package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;

/** Schema for a 64 bit integer */
public class LongSchema extends RangeSchema<JsonLong>
{
  // -- schema parameters -------------------------------------------------------------------------
  
  private static Parameters parameters = null; 
  
  public static Parameters getParameters()
  {
    if (parameters == null)
    {
      Schema schema = new LongSchema();
      parameters = new Parameters(
          new JsonString[] { PAR_MIN, PAR_MAX, PAR_VALUE },
          new Schema[]     { schema , schema , schema    },
          new JsonValue[]  { null   , null   , null      });
    }
    return parameters;
  }
  
  
  // -- construction ------------------------------------------------------------------------------
  
  public LongSchema(JsonRecord args)
  {
    this(
        (JsonNumeric)getParameters().argumentOrDefault(PAR_MIN, args),
        (JsonNumeric)getParameters().argumentOrDefault(PAR_MAX, args),
        (JsonNumeric)getParameters().argumentOrDefault(PAR_VALUE, args));
  }
  
  public LongSchema()
  {
  }
  
  public LongSchema(JsonLong min, JsonLong max, JsonLong value)
  {
    super(min, max, value);
  }
  
  public LongSchema(JsonNumeric min, JsonNumeric max, JsonNumeric value)
  {
    this(convert(min), convert(max), convert(value));
  }
  
  /** Convert the specified numeric to a long or throw an exception */  
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
  
  
  // -- Schema methods ----------------------------------------------------------------------------

  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.LONG;
  }

  @Override
  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonLong || value instanceof JsonDecimal))
    {
      return false;
    }
    if (value instanceof JsonDecimal)
    {
      try 
      {
        ((JsonDecimal)value).longValueExact();
      }
      catch (ArithmeticException e)
      {
        return false;
      }
    }
    // value can be long or decimal, min and max are long

    if (this.value != null)
    {
      return value.equals(this.value);
    }

    if ( (min != null && value.compareTo(min)<0) || (max != null && value.compareTo(max)>0) )
    {
      return false;
    }
    
    return true;
  }
}
