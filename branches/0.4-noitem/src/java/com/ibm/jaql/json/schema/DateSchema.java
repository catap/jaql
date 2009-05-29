package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;

/** Schema for a date value */
public class DateSchema extends RangeSchema<JsonDate>
{
  // -- schema parameters -------------------------------------------------------------------------
  
  private static Parameters parameters = null; 
  
  public static Parameters getParameters()
  {
    if (parameters == null)
    {
      Schema schema = new DateSchema();
      parameters = new Parameters(
          new JsonString[] { PAR_MIN, PAR_MAX, PAR_VALUE },
          new Schema[]     { schema , schema , schema    },
          new JsonValue[]  { null   , null   , null      });
    }
    return parameters;
  }
  
  
  // -- construction ------------------------------------------------------------------------------
  
  public DateSchema(JsonRecord args)
  {
    this(
        (JsonDate)getParameters().argumentOrDefault(PAR_MIN, args),
        (JsonDate)getParameters().argumentOrDefault(PAR_MAX, args),
        (JsonDate)getParameters().argumentOrDefault(PAR_VALUE, args));
  }
  
  public DateSchema()
  {
  }
  
  public DateSchema(JsonDate min, JsonDate max, JsonDate value)
  {
    super(min, max, value);
  }
  
  
  // -- Schema methods ----------------------------------------------------------------------------

  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.DATE;
  }

  @Override
  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonDate))
    {
      return false;
    }

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
