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


/** A double JSON value (64-bit base-2 floating point value). 
 * 
 * Instances of this class are immutable, but subclasses might add mutation functionality
 * (in which case they have to override the {@link #getCopy(JsonValue)} method). 
 */
public class JsonDouble extends JsonNumber
{
  protected double value = 0;

  // -- construction ------------------------------------------------------------------------------

  /** Copy constructs from the specified value. */
  public JsonDouble(double value)
  {
    this.value = value;
  }

  /** Copy constructs from the specified value. */
  public JsonDouble(JsonDouble value)
  {
    this.value = value.value;
  }

  /** Constructs a new <code>JsonDouble</code> from the specified value. 
  *
  * @throws NumberFormatException when <code>value</code> does not represent a valid double
  */
  public JsonDouble(String value) throws NumberFormatException
  {
    this.value = Double.parseDouble(value);
  }

  // -- getters -----------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonNumeric#decimalValue() */
  @Override
  public BigDecimal decimalValue()
  {
    return new BigDecimal(value, MathContext.DECIMAL128);
  }
  
  /* @see com.ibm.jaql.json.type.JsonNumeric#decimalValueExact() */
  @Override
  public BigDecimal decimalValueExact()
  {
    BigDecimal x = new BigDecimal(value, MathContext.DECIMAL128);
    if (x.doubleValue() != value)
    {
      throw new ArithmeticException(this + " cannot be represented as an 128-bit decimal");
    }
    return x;
  }


  /* @see com.ibm.jaql.json.type.JsonNumeric#doubleValue() */
  @Override
  public double doubleValue()
  {
    return value;
  }
  
  /* @see com.ibm.jaql.json.type.JsonNumeric#doubleValueExact() */
  @Override
  public double doubleValueExact()
  {
    return value;
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#intValue() */
  @Override
  public int intValue()
  {
    return (int) value;
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#intValueExact() */
  @Override
  public int intValueExact() throws ArithmeticException
  {
    int x = (int) value;
    if (x != value) // TODO: is this the best way to determine exactness?
    {
      throw new ArithmeticException(this + " cannot be represented as an integer");
    }
    return x;
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#longValue() */
  @Override
  public long longValue()
  {
    return (long) value;
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#longValueExact() */
  @Override
  public long longValueExact() throws ArithmeticException
  {
    long x = (long) value;
    if (x != value) // TODO: is this the best way to determine exactness?
    {
      throw new ArithmeticException(this + " cannot be represented as a long integer");
    }
    return x;
  }

  /** Returns {@link #doubleValue()}. */
  public double get()
  {
    return value;
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public JsonDouble getCopy(JsonValue target) throws Exception
  {
    return this;
  }
  
  @Override
  public JsonDouble getImmutableCopy() throws Exception
  {
    return this;
  }
  
  // -- comparison/hashing ------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    JsonNumber other = (JsonNumber)x;
    JsonType otherType = other.getEncoding().getType();
    switch (otherType)
    {
    case LONG:
      return compare(this, (JsonLong)other);
    case DOUBLE:
      return compare(this, (JsonDouble)other);
    case DECFLOAT:
      return compare(this, (JsonDecimal)other);
    default:
      throw new IllegalStateException("unknown numeric type " + otherType);
    }
  }

  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    long x = (long)value;
    if (x == value)
    {
      return JsonLong.longHashCode(x);
    }
    x = Double.doubleToLongBits(value);
    return JsonLong.longHashCode(x);
  }
  
  // -- misc --------------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.DOUBLE;
  }
}
