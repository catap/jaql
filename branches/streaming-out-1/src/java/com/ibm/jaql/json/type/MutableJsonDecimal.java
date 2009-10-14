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
package com.ibm.jaql.json.type;

import java.math.BigDecimal;


/** A mutable decfloat JSON value (128-bit base-10 floating point value). */
public class MutableJsonDecimal extends JsonDecimal
{
  // -- construction ------------------------------------------------------------------------------

  /** Constructs a new <code>MutableJsonDecimal</code> representing 0. */
  public MutableJsonDecimal()
  {
    super(BigDecimal.ZERO);
  }

  /** @see JsonDecimal#JsonDecimal(BigDecimal) */
  public MutableJsonDecimal(BigDecimal value)
  {
    super(value);
  }
  
  /** @see JsonDecimal#JsonDecimal(JsonDecimal) */
  public MutableJsonDecimal(JsonDecimal value)
  {
    super(value);
  }

  /** @see JsonDecimal#JsonDecimal(String) */
  public MutableJsonDecimal(String value) throws NumberFormatException
  {
    super(value);
  }

  /** @see JsonDecimal#JsonDecimal(long) */
  public MutableJsonDecimal(long value)
  {
    super(value);
  }

  // -- getters -----------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public MutableJsonDecimal getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    if (target instanceof MutableJsonDecimal)
    {
      MutableJsonDecimal t = (MutableJsonDecimal)target;
      t.value = this.value; // BigDecimal is immutable --> can share
      return t;
    }
    return new MutableJsonDecimal(value);
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#getImmutableCopy() */
  @Override
  public JsonDecimal getImmutableCopy() 
  {
    return new JsonDecimal(this);
  }
  
  // -- mutation ----------------------------------------------------------------------------------
  
  /** Sets the value of this <code>JsonDecimal</code> to the specified value. */
  public void set(BigDecimal value)
  {
    this.value = value;
  }

  /** Sets the value of this <code>JsonDecimal</code> to the specified value. */
  public void set(long value)
  {
    this.value = new BigDecimal(value);
  }

  /** Negates this value. */
  public void negate()
  {
    value = value.negate();
  }
}
