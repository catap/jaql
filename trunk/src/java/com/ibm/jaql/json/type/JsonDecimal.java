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
import java.math.MathContext;


/** A decfloat JSON value (128-bit base-10 floating point value). 
 * 
 * Instances of this class are immutable, but subclasses might add mutation functionality
 * (in which case they have to override the {@link #getCopy(JsonValue)} method). 
 */
public class JsonDecimal extends JsonNumber
{
  protected BigDecimal value;

  // -- construction ------------------------------------------------------------------------------

  /** Copy constructs from the specified value. */
  public JsonDecimal(BigDecimal value)
  {
    this.value = value.round(MathContext.DECIMAL128); // cheap
  }

  /** Copy constructs from the specified value. */
  public JsonDecimal(JsonDecimal value)
  {
    this.value = value.value; // BigDecimal is immutable --> can share
  }

  /** Copy constructs from the specified value.
   *
   * @throws NumberFormatException when the specified value cannot be parsed
   */
  public JsonDecimal(String value) throws NumberFormatException
  {
    this.value = new BigDecimal(value, MathContext.DECIMAL128);
  }

  /** Copy constructs from the specified value. */
  public JsonDecimal(long value)
  {
    this.value = new BigDecimal(value, MathContext.DECIMAL128);
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

  /* @see com.ibm.jaql.json.type.JsonNumeric#decimalValueExact() */
  @Override
  public BigDecimal decimalValueExact()
  {
    return value;
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#doubleValue() */
  @Override
  public double doubleValue()
  {
    return value.doubleValue();
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#doubleValueExact() */
  @Override
  public double doubleValueExact()
  {
    double x = value.doubleValue(); 
    if (!new BigDecimal(x, MathContext.DECIMAL128).equals(value))
    {
      throw new ArithmeticException(this + " cannot be represented as a double");
    }
    return x;
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
    return this;
  }
  
  @Override
  public JsonDecimal getImmutableCopy() throws Exception
  {
    return this;
  }
  
  // -- comparison/hashing ------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    JsonNumeric other = (JsonNumeric)x;
    return compare(this, other);
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
      long x = Double.doubleToLongBits(doubleValue()); // to ensure consistency with double
      return JsonLong.longHashCode(x);
    }
  }
  
  // -- misc --------------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.DECFLOAT;
  }
}
