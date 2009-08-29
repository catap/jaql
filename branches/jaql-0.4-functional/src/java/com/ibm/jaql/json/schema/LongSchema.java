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

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/** Schema for a 64 bit integer */
public final class LongSchema extends RangeSchema<JsonLong>
{
  // -- schema parameters -------------------------------------------------------------------------
  
  private static JsonValueParameters parameters = null; 
  
  public static JsonValueParameters getParameters()
  {
    if (parameters == null)
    {
      Schema schema = SchemaFactory.longOrNullSchema();
      parameters = new JsonValueParameters(
          new JsonValueParameter(PAR_MIN, schema, null),
          new JsonValueParameter(PAR_MAX, schema, null),
          new JsonValueParameter(PAR_VALUE, schema, null));
    }
    return parameters;
  }
  
  // -- construction ------------------------------------------------------------------------------
  
  public LongSchema(JsonRecord args)
  {
    this(
        (JsonNumber)getParameters().argumentOrDefault(PAR_MIN, args),
        (JsonNumber)getParameters().argumentOrDefault(PAR_MAX, args),
        (JsonNumber)getParameters().argumentOrDefault(PAR_VALUE, args));
  }
  
  LongSchema()
  {
  }
  
  public LongSchema(JsonLong min, JsonLong max, JsonLong value)
  {
    init(min, max, value);
  }
  
  public LongSchema(JsonNumber min, JsonNumber max, JsonNumber value)
  {
    this(convert(min), convert(max), convert(value));
  }
  
  /** Convert the specified numeric to a long or throw an exception */  
  private static JsonLong convert(JsonNumber v)
  {
    if (v == null) return null;
    if (v instanceof JsonNumber)
    {
      try 
      {
        return new JsonLong(((JsonNumber)v).longValueExact());
      }
      catch (ArithmeticException e)
      {
        // throw below
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

  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    return new Class[] { JsonLong.class }; 
  }
  
  @Override
  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonLong))
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
    if (other instanceof LongSchema)
    {
      LongSchema o = (LongSchema)other;
      JsonLong min = SchemaUtil.minOrValue(this.min, o.min, this.value, o.value);
      JsonLong max = SchemaUtil.maxOrValue(this.max, o.max, this.value, o.value);      
      return new LongSchema(min, max, null);
    }
    return null;
  }
}
