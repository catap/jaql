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

import com.ibm.jaql.util.BaseUtil;

/** A long JSON value (64-bit signed integer). 
 * 
 * Instances of this class are immutable, but subclasses might add mutation functionality
 * (in which case they have to override the {@link #getCopy(JsonValue)} method). 
 */
public class JsonLong extends JsonNumber
{
  protected long value = 0;

  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs a new <code>JsonLong</code> with the specified value. */
  public JsonLong(long value)
  {
    this.value = value;
  }

  /** Copy constructs from the specified value. */
  public JsonLong(JsonLong value)
  {
    this.value = value.get();
  }
  
  /** Constructs a new <code>JsonLong</code> from the specified value. 
  *
  * @throws NumberFormatException when <code>value</code> does not represent a valid long
  */
  public JsonLong(String value) 
  {
    this(parseLong(value));
  }
  
  /** Constructs a new <code>JsonDecimal</code> from the specified value. 
  *
  * @throws NumberFormatException when <code>value</code> does not represent a valid long
  */
  public JsonLong(JsonString value) 
  {
    this(parseLong(value));
  }
  
  /** Returns an (immutable) {@link JsonLong} for the given value. Recommended for common
   * integers because instances can be shared. */
  public static JsonLong make(long i)
  {
    if (i == -1)
    {
      return JsonLong.MINUS_ONE;
    }
    if (i == 0)
    {
      return JsonLong.ZERO;
    }
    if (i == 1)
    {
      return JsonLong.ONE;
    }
    return new JsonLong(i);
  }

  public static long parseLong(String s)
  {
    s = s.trim();
    if (s.startsWith("0x") || s.startsWith("0X"))
    {
      // parseLong can't handle negative hex values - do we want it to?
      return Long.parseLong(s.substring(2), 16);
    }
    else
    {
      return Long.parseLong(s);
    }
  }
  
  public static long parseLong(JsonString s)
  {
    // TODO: make more efficient
    return parseLong(s.toString());
  }
  
  // -- getters -----------------------------------------------------------------------------------
  
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
    if (x != value)
    {
      throw new ArithmeticException(this + " cannot be represented as an int");
    }
    return x;
  }

  /** @see {@link #longValue()} */
  @Override
  public long longValue()
  {
    return value;
  }
  
  /* @see com.ibm.jaql.json.type.JsonNumeric#longValueExact() */
  @Override
  public long longValueExact()
  {
    return value;
  }

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
    return new BigDecimal(value, MathContext.DECIMAL128);
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
    double x = value;
    if ((long)x != value)
    {
      throw new ArithmeticException(this + " cannot be represented as a double");
    }
    return x;
  }
  
  /** Returns {@link #longValue()}. */
  public long get()
  {
    return value;
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public JsonLong getCopy(JsonValue target) 
  {
    return this;
  }
  
  @Override
  public JsonLong getImmutableCopy() 
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
    return longHashCode(this.value);
  }

  /** Static utility methods for determining the long hash code of a long value. */
  public static long longHashCode(long value)
  {
    return value * BaseUtil.GOLDEN_RATIO_64;
  }

  
  // -- misc --------------------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.LONG;
  }

}
