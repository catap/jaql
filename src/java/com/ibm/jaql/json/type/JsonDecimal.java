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


/** A decfloat JSON value (128-bit base-10 floating point value). */
public class JsonDecimal extends JsonNumber
{
  protected BigDecimal value;

  // -- construction ------------------------------------------------------------------------------

  /** Constructs a new <code>JsonDecimal</code> representing an undefined value. */
  public JsonDecimal()
  {
  }

  /** Constructs a new <code>JsonDecimal</code> representing the specified value. */
  public JsonDecimal(BigDecimal value)
  {
    this.value = value;
  }

  /** Constructs a new <code>JsonDecimal</code> representing the specified value.
   *
   * @throws NumberFormatException when the specified value cannot be parsed
   */
  public JsonDecimal(String value) throws NumberFormatException
  {
    this.value = new BigDecimal(value);
  }

  /** Constructs a new <code>JsonDecimal</code> representing the specified value. */
  public JsonDecimal(long value)
  {
    this.value = new BigDecimal(value);
  }

  // -- getters -----------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonNumeric#intValue() */
  @Override
  public int intValue()
  {
    return value.intValue();
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#intValueExact() */
  @Override
  public int intValueExact() throws ArithmeticException
  {
    return value.intValueExact();
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#longValue() */
  @Override
  public long longValue()
  {
    return value.longValue();
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#longValueExact() */
  @Override
  public long longValueExact() throws ArithmeticException
  {
    return value.longValueExact();
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#decimalValue() */
  @Override
  public BigDecimal decimalValue()
  {
    return value;
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#doubleValue() */
  @Override
  public double doubleValue()
  {
    return value.doubleValue();
  }

  /** Returns {@link #decimalValue()}. */
  public BigDecimal get()
  {
    return value;
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public JsonDecimal getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    if (target instanceof JsonDecimal)
    {
      JsonDecimal t = (JsonDecimal)target;
      t.value = this.value; // BigDecimal is immutable --> can share
      return t;
    }
    return new JsonDecimal(value);
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

  // -- comparison/hashing ------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    if (x instanceof JsonDecimal)
    {
      return value.compareTo(((JsonDecimal) x).value);
    }
    else
    {
      // TODO: this could be faster
      long y = ((JsonLong) x).value;
      return value.compareTo(new BigDecimal(y));
    }
  }

  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    // TODO: we need a better BigDecimal.  BigDecimal.hashCode() does not work for us.
    // h(1) != h(1.0), where 1 is a JLong 
    // h(1.0) != h(1.00) because BigDecimal says so.
    // So for now:
    //   if we can convert to a long value, we use it
    //   otherwise, create a canonical string representation and hash that.
    try
    {
      // If the value is a long, then use the JLong hashCode.
      long x = value.longValueExact();
      return JsonLong.longHashCode(x);
    }
    catch (ArithmeticException ex)
    {
      BigDecimal canonical = value.stripTrailingZeros();
      return canonical.hashCode();
    }
  }
  
  
  // -- misc --------------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.DECIMAL;
  }
}
