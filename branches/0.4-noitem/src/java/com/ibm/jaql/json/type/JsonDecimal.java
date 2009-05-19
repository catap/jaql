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


/**
 * 
 */
public class JsonDecimal extends JsonNumber
{
  public BigDecimal value;

  /**
   * 
   */
  public JsonDecimal()
  {
  }

  /**
   * @param value
   */
  public JsonDecimal(BigDecimal value)
  {
    this.value = value;
  }

  /**
   * @param value
   */
  public JsonDecimal(String value)
  {
    this.value = new BigDecimal(value);
  }

  /**
   * @param value
   */
  public JsonDecimal(long value)
  {
    this.value = new BigDecimal(value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.DECIMAL;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#intValue()
   */
  @Override
  public int intValue()
  {
    return value.intValue();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#intValueExact()
   */
  @Override
  public int intValueExact()
  {
    return value.intValueExact();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#longValue()
   */
  @Override
  public long longValue()
  {
    return value.longValue();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#longValueExact()
   */
  @Override
  public long longValueExact()
  {
    return value.longValueExact();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#decimalValue()
   */
  @Override
  public BigDecimal decimalValue()
  {
    return value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#doubleValue()
   */
  @Override
  public double doubleValue()
  {
    return value.doubleValue();
  }

  /**
   * @param value
   */
  public void setValue(BigDecimal value)
  {
    this.value = value;
  }

  /**
   * @param value
   */
  public void setValue(long value)
  {
    this.value = new BigDecimal(value);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JNumeric#negate()
   */
  @Override
  public void negate()
  {
    value = value.negate();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JsonValue jvalue)
  {
    JsonDecimal d = (JsonDecimal) jvalue;
    value = d.value; // shared because it is immutable
  }
}
