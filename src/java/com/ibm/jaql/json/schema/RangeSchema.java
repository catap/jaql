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

import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;

/** Superclass for schemata with minimum, maximum and constant value. */
public abstract class RangeSchema<T extends JsonValue> extends Schema
{
  protected T min;              // minimum value
  protected T max;              // maximum value
  protected T value;            // actual value

  // -- construction ------------------------------------------------------------------------------

  RangeSchema()
  {
  }
  
  @SuppressWarnings("unchecked")
  protected void init(T min, T max, T value)
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
      this.value = (T)JsonUtil.getImmutableCopyUnchecked(value);
    }
    else if (JsonUtil.equals(min, max))
    {
      this.value = (T)JsonUtil.getImmutableCopyUnchecked(min);
      // min and max stay null
    }
    else
    {
      this.min = (T)JsonUtil.getImmutableCopyUnchecked(min);
      this.max = (T)JsonUtil.getImmutableCopyUnchecked(max);
    }
  }
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public boolean isConstant()
  {
    return value != null || (min != null && max != null && min.equals(max));
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

  
  // -- comparison --------------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Schema other)
  {
    int c = this.getSchemaType().compareTo(other.getSchemaType());
    if (c != 0) return c;
    
    RangeSchema<T> o = (RangeSchema<T>)other;
    c = SchemaUtil.compare(this.value, o.value);
    if (c != 0) return c;
    c = SchemaUtil.compare(this.min, o.min);
    if (c != 0) return c;
    c = SchemaUtil.compare(this.max, o.max);
    if (c != 0) return c;
    
    return 0;
  }
}
