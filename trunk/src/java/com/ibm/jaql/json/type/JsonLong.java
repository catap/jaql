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

import com.ibm.jaql.util.BaseUtil;

/** A long JSON value (64-bit signed integer). */
public class JsonLong extends JsonNumber
{
  protected long value = 0;

  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs a new <code>JsonLong</code> having value 0. */
  public JsonLong()
  {
  }

  /** Constructs a new <code>JsonLong</code> with the specified value. */
  public JsonLong(long value)
  {
    this.value = value;
  }

  /** Constructs a new <code>JsonLong</code> from the specified value. 
   *
   * @throws NumberFormatException when <code>value</code> does not represent a valid long
   */
  public JsonLong(String value) throws NumberFormatException
  {
    if (value.startsWith("0x") || value.startsWith("0X"))
    {
      // parseLong can't handle negative hex values - do we want it to?
      this.value = Long.parseLong(value.substring(2), 16);
      //      int n = value.length();
      //      if( n < 2 || n > 18 )
      //      {
      //        throw new RuntimeException("invalid hex string length");
      //      }
      //      long v = 0;
      //      for(int i = 2 ; i < n ; i++)
      //      {
      //        v <<= 4;
      //        char c = value.charAt(i);
      //        if( c >= '0' || c <= '9' )
      //        {
      //          v += c - '0'; 
      //        }
      //        else if( c >= 'A' || c <= 'F' )
      //        {
      //          v += c - 'A'; 
      //        }
      //        else if( c >= 'a' || c <= 'F' )
      //        {
      //          v += c - 'a'; 
      //        }
      //        else
      //        {
      //          throw new RuntimeException("invalid hex string character");
      //        }
      //      }
      //      this.value = v;
    }
    else
    {
      this.value = Long.parseLong(value);
    }
  }

  /** Returns a {@link JsonLong} for the given value. The returned value must not be changed;
   * is mutation is required, use one of the constructors instead. */
  public static JsonValue makeShared(long i)
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
      throw new ArithmeticException("cannot convert to int: " + value);
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
    return new BigDecimal(value);
  }

  /* @see com.ibm.jaql.json.type.JsonNumeric#doubleValue() */
  @Override
  public double doubleValue()
  {
    return value;
  }
  
  /** Returns {@link #longValue()}. */
  public long get()
  {
    return value;
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public JsonLong getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    if (target instanceof JsonLong)
    {
      JsonLong t = (JsonLong)target;
      t.value = this.value;
      return t;
    }
    return new JsonLong(value);
  }
  

  // -- setters -----------------------------------------------------------------------------------

  /** Sets the value of this <code>JsonLong</code> to the specified value. */
  public void set(long value)
  {
    this.value = value;
  }
  

  /** Negates this JsonLong. This method does not produce meaningful results when it represents
   * {@link Long#MAX_VALUE}. */
  public void negate()
  {
    value = -value;
  }

  // -- comparison/hashing ------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    //    int c = Util.typeCompare(this, (Writable)x);
    //    if( c != 0 )
    //    {
    //      return c;
    //    }
    if (x instanceof JsonLong)
    {
      JsonLong y = (JsonLong) x;
      return this.value < y.value ? -1 : ((this.value > y.value) ? +1 : 0);
    }
    else
    {
      return -((JsonDecimal) x).compareTo(this);
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
