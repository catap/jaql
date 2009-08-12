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
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonDecimal;
import com.ibm.jaql.lang.expr.core.Parameters;

/** Schema for a 128 bit decimal float value */
public final class DecfloatSchema extends RangeSchema<JsonDecimal>
{
  // -- schema parameters -------------------------------------------------------------------------
  
  private static Parameters parameters = null; 
  
  public static Parameters getParameters()
  {
    if (parameters == null)
    {
      Schema schema = new DecfloatSchema();
      parameters = new Parameters(
          new JsonString[] { PAR_MIN, PAR_MAX, PAR_VALUE },
          new Schema[]     { schema , schema , schema    },
          new JsonValue[]  { null   , null   , null      });
    }
    return parameters;
  }

  // used for matching
  private MutableJsonDecimal temp = new MutableJsonDecimal();
  
  //-- construction ------------------------------------------------------------------------------
  
  public DecfloatSchema(JsonRecord args)
  {
    this(
        (JsonNumber)getParameters().argumentOrDefault(PAR_MIN, args),
        (JsonNumber)getParameters().argumentOrDefault(PAR_MAX, args),
        (JsonNumber)getParameters().argumentOrDefault(PAR_VALUE, args));
  }
  
  DecfloatSchema()
  {
  }
  
  public DecfloatSchema(JsonDecimal min, JsonDecimal max, JsonDecimal value)
  {
    init(min, max, value);
  }
  
  public DecfloatSchema(JsonNumber min, JsonNumber max, JsonNumber value)
  {
    this(convert(min), convert(max), convert(value));
  }
  
  /** Convert the specified numeric to a decimal or throw an exception */  
  private static JsonDecimal convert(JsonNumber v)
  {
    if (v == null) return null;
    if (v instanceof JsonNumber)
    {
      try 
      {
        return new JsonDecimal(((JsonNumber)v).decimalValueExact());
      }
      catch (ArithmeticException e)
      {
        // throw below
      }
    }
    throw new IllegalArgumentException("interval argument has to be of type decfloat: " + v);
  }
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.DECFLOAT;
  }
  
  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    return new Class[] { JsonDecimal.class, JsonLong.class, JsonDouble.class }; 
  }

  public boolean matches(JsonValue value)
  {
    if (value == null || !value.getType().isNumber())
    {
      return false;
    }
    
    // convert to decimal
    try 
    {
      temp.set( ((JsonNumber)value).decimalValueExact() );
    }
    catch (ArithmeticException e)
    {
      return false;
    }

    // match
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
  
  // -- merge -------------------------------------------------------------------------------------

  @Override
  protected Schema merge(Schema other)
  {
    if (other instanceof DecfloatSchema)
    {
      DecfloatSchema o = (DecfloatSchema)other;
      JsonDecimal min = SchemaUtil.minOrValue(this.min, o.min, this.value, o.value);
      JsonDecimal max = SchemaUtil.maxOrValue(this.max, o.max, this.value, o.value);
      return new DecfloatSchema(min, max, null);
    }
    return null;
  }
}
