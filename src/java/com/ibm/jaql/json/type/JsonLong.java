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

/**
 * 
 */
public class JsonLong extends JsonNumber
{
  public long value;

  /**
   * 
   */
  public JsonLong()
  {
  }

  /**
   * @param value
   */
  public JsonLong(long value)
  {
    this.value = value;
  }

  /**
   * @param value
   */
  public JsonLong(String value)
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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.LONG;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#intValue()
   */
  @Override
  public int intValue()
  {
    return (int) value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#intValueExact()
   */
  @Override
  public int intValueExact()
  {
    int x = (int) value;
    if (x != value)
    {
      throw new ArithmeticException("cannot convert to int: " + value);
    }
    return x;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#longValue()
   */
  @Override
  public long longValue()
  {
    return value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#longValueExact()
   */
  @Override
  public long longValueExact()
  {
    return value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#decimalValue()
   */
  @Override
  public BigDecimal decimalValue()
  {
    return new BigDecimal(value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#doubleValue()
   */
  @Override
  public double doubleValue()
  {
    return value;
  }

  /**
   * @param value
   */
  public void setValue(long value)
  {
    this.value = value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#negate()
   */
  @Override
  public void negate()
  {
    value = -value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
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

  /**
   * @param value
   * @return
   */
  public static long longHashCode(long value)
  {
    return value * BaseUtil.GOLDEN_RATIO_64;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    return longHashCode(this.value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JsonValue jvalue) throws Exception
  {
    JsonLong num = (JsonLong) jvalue;
    value = num.value;
  }

  /**
   * @param i
   * @return
   */
  public static JsonValue sharedLong(int i)
  {
    switch (i)
    {
      case -1 :
        return JsonLong.MINUS_ONE;
      case 0 :
        return JsonLong.ZERO;
      case 1 :
        return JsonLong.ONE;
      default :
        return new JsonLong(i);
    }
  }
}
