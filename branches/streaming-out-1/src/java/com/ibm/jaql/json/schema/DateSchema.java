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

import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/** Schema for a date value */
public final class DateSchema extends RangeSchema<JsonDate>
{
  // -- schema parameters -------------------------------------------------------------------------
  
  private static JsonValueParameters parameters = null; 
  
  public static JsonValueParameters getParameters()
  {
    if (parameters == null)
    {
      Schema schema = SchemaFactory.dateOrNullSchema();
      parameters = new JsonValueParameters(
          new JsonValueParameter(PAR_MIN, schema, null),
          new JsonValueParameter(PAR_MAX, schema, null),
          new JsonValueParameter(PAR_VALUE, schema, null));
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
  
  DateSchema()
  {
  }
  
  public DateSchema(JsonDate min, JsonDate max, JsonDate value)
  {
    init(min, max, value);
  }
  
  
  // -- Schema methods ----------------------------------------------------------------------------

  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.DATE;
  }

  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    return new Class[] { JsonDate.class }; 
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
  
  // -- merge -------------------------------------------------------------------------------------

  @Override
  protected Schema merge(Schema other)
  {
    if (other instanceof DateSchema)
    {
      DateSchema o = (DateSchema)other;
      JsonDate min = SchemaUtil.minOrValue(this.min, o.min, this.value, o.value);
      JsonDate max = SchemaUtil.maxOrValue(this.max, o.max, this.value, o.value);
      return new DateSchema(min, max, null);
    }
    return null;
  }
}
