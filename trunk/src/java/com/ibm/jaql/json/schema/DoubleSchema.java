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

import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;

/** Schema for a double. */
public class DoubleSchema extends RangeSchema<JsonDouble>
{
  // -- schema parameters -------------------------------------------------------------------------
  
  private static Parameters parameters = null; 
  
  public static Parameters getParameters()
  {
    if (parameters == null)
    {
      Schema schema = new DoubleSchema();
      parameters = new Parameters(
          new JsonString[] { PAR_MIN, PAR_MAX, PAR_VALUE },
          new Schema[]     { schema , schema , schema    },
          new JsonValue[]  { null   , null   , null      });
    }
    return parameters;
  }

  
  // -- construction ------------------------------------------------------------------------------
  
  public DoubleSchema(JsonRecord args)
  {
    this(
        (JsonNumeric)getParameters().argumentOrDefault(PAR_MIN, args),
        (JsonNumeric)getParameters().argumentOrDefault(PAR_MAX, args),
        (JsonNumeric)getParameters().argumentOrDefault(PAR_VALUE, args));
  }
  
  DoubleSchema()
  {
  }
  
  public DoubleSchema(JsonDouble min, JsonDouble max, JsonDouble value)
  {
    super(min, max, value);
  }

  public DoubleSchema(JsonNumeric min, JsonNumeric max, JsonNumeric value)
  {
    this(convert(min), convert(max), convert(value));
  }
  
  /** Convert the specified numeric to a long or throw an exception */  
  private static JsonDouble convert(JsonNumeric v)
  {
    if (v == null || v instanceof JsonDouble)
    {
      return (JsonDouble)v;
    }
    throw new IllegalArgumentException("interval argument has to be of type double: " + v);
  }
  
  
  // -- Schema methods ----------------------------------------------------------------------------

  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.DOUBLE;
  }

  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    return new Class[] { JsonDouble.class }; 
  }
  
  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonDouble))
    {
      return false;
    }
    // value is double, as are min and max
    
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
    if (other instanceof DoubleSchema)
    {
      DoubleSchema o = (DoubleSchema)other;
      JsonDouble min = SchemaUtil.minOrValue(this.min, o.min, this.value, o.value);
      JsonDouble max = SchemaUtil.maxOrValue(this.max, o.max, this.value, o.value);      
      return new DoubleSchema(min, max, null);
    }
    return null;
  }
}
