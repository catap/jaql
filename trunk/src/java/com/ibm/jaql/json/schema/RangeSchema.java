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

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.Bool3;

/** Superclass for schemata with minimum, maximum and constant value. */
public abstract class RangeSchema<T extends JsonValue> extends Schema
{
  protected T min;              // minimum value
  protected T max;              // maximum value
  protected T value;            // actual value

  // -- construction ------------------------------------------------------------------------------

  public RangeSchema()
  {
  }
  
  public RangeSchema(T min, T max, T value)
  {
    // check min/max
    if (!SchemaUtil.checkInterval(min, max))
    {
      throw new IllegalArgumentException("invalid interval: " + min + " " + max);
    }
    
    // check value
    if (value != null)
    {
      this.min = min; // set to be used by matches
      this.max = max;
      if (!matches(value))
      {
        throw new IllegalArgumentException("value argument conflicts with other arguments");
      }
      this.min = this.max = null;
      this.value = value;
    }
    else if (JsonValue.equals(min, max))
    {
      this.value = min;
      // this.min and this.max stay null
    }
    else
    {
      this.min = min;
      this.max = max;
    }
  }
  
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  @Override
  public Bool3 isConst()
  {
    if (min != null && max != null && min.equals(max))
    {
      return Bool3.TRUE;
    }
    return Bool3.UNKNOWN;
  }

  @Override
  public Bool3 isArray()
  {
    return Bool3.FALSE;
  }

  @Override
  public abstract boolean matches(JsonValue value);
  
  
  // -- getters -----------------------------------------------------------------------------------
  
  public final T getMin()
  {
    return min;
  }
  
  public final T getMax()
  {
    return max;
  }
  
  public final T getValue()
  {
    return value;
  }
}
