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


/** A double JSON value (64-bit base-2 floating point value). */
public class JsonDouble extends JsonNumeric
{
  protected double value = 0;

  // -- construction ------------------------------------------------------------------------------

  /** Constructs a new <code>JsonDouble</code> having value 0. */
  public JsonDouble()
  {
  }

  /** Constructs a new <code>JsonDouble</code> with the specified value. */
  public JsonDouble(double value)
  {
    this.value = value;
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
    return new BigDecimal(value);
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#doubleValue() */
  @Override
  public double doubleValue()
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
      throw new ArithmeticException("cannot convert to int: " + value);
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
      throw new ArithmeticException("cannot convert to long exactly: " + value);
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
    if (target == this) target = null;
    
    if (target instanceof JsonDouble)
    {
      JsonDouble t = (JsonDouble)target;
      t.value = this.value;
      return t;
    }
    return new JsonDouble(value);
  }
  
  // -- mutation ----------------------------------------------------------------------------------

  /** Negates this value */ 
  public void negate()
  {
    value = -value;
  }
  
  /** Sets the value of this <code>JsonDouble</code> to the specified value. */
  public void set(double value)
  {
    this.value = value;
  }

  // -- comparison/hashing ------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object obj)
  {
    JsonDouble that = (JsonDouble) obj;
    if (this.value < that.value)
    {
      return -1;
    }
    else if (this.value > that.value)
    {
      return +1;
    }
    else
    {
      return 0;
    }
  }

  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    long x = Double.doubleToLongBits(value);
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
