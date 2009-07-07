/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;

/** Schema for a 128 bit decimal float value */
public class DecimalSchema extends RangeSchema<JsonDecimal>
{
  // -- schema parameters -------------------------------------------------------------------------
  
  private static Parameters parameters = null; 
  
  public static Parameters getParameters()
  {
    if (parameters == null)
    {
      Schema schema = new DecimalSchema();
      parameters = new Parameters(
          new JsonString[] { PAR_MIN, PAR_MAX, PAR_VALUE },
          new Schema[]     { schema , schema , schema    },
          new JsonValue[]  { null   , null   , null      });
    }
    return parameters;
  }

  
  //-- construction ------------------------------------------------------------------------------
  
  public DecimalSchema(JsonRecord args)
  {
    this(
        (JsonNumeric)getParameters().argumentOrDefault(PAR_MIN, args),
        (JsonNumeric)getParameters().argumentOrDefault(PAR_MAX, args),
        (JsonNumeric)getParameters().argumentOrDefault(PAR_VALUE, args));
  }
  
  DecimalSchema()
  {
  }
  
  public DecimalSchema(JsonDecimal min, JsonDecimal max, JsonDecimal value)
  {
    super(min, max, value);
  }
  
  public DecimalSchema(JsonNumeric min, JsonNumeric max, JsonNumeric value)
  {
    this(convert(min), convert(max), convert(value));
  }
  
  /** Convert the specified numeric to a decimal or throw an exception */  
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
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.DECFLOAT;
  }

  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonLong || value instanceof JsonDecimal))
    {
      return false;
    }
    // value can be long or decimal, min and max are decimal
    
    if (this.value != null)
    {
      return value.equals(this.value);
    }

    if ( (min != null && min.compareTo(value)>0) || (max != null && max.compareTo(value)<0) )
    {
      return false;
    }
    
    return true;
  }
  
  // -- merge -------------------------------------------------------------------------------------

  @Override
  protected Schema merge(Schema other)
  {
    if (other instanceof DecimalSchema)
    {
      DecimalSchema o = (DecimalSchema)other;
      JsonDecimal min = SchemaUtil.minOrValue(this.min, o.min, this.value, o.value);
      JsonDecimal max = SchemaUtil.maxOrValue(this.max, o.max, this.value, o.value);
      return new DecimalSchema(min, max, null);
    }
    return null;
  }
}
