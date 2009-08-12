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

/** The base of all number types. */
public abstract class JsonNumber extends JsonAtom
{
  public final static JsonLong ZERO           = new JsonLong(0);
  public final static JsonLong ONE            = new JsonLong(1);
  public final static JsonLong MINUS_ONE      = new JsonLong(-1);
  
  // -- getters -----------------------------------------------------------------------------------
  
  /** If this numeric value is representable as an integer (after rounding), returns it as an 
   * integer value. Otherwise the result is undefined. */
  public abstract int intValue();

  /** If this numeric value is representable as an integer (after rounding), returns it as an 
   * integer value. Otherwise throws an {@link ArithmeticException}. */
  public abstract int intValueExact() throws ArithmeticException;

  /** If this numeric value is representable as a long (after rounding), returns it as a 
   * long value. Otherwise the result is undefined. */
  public abstract long longValue();

  /** If this numeric value is representable as a long (after rounding), returns it as a 
   * long value. Otherwise throws an {@link ArithmeticException}. */
  public abstract long longValueExact() throws ArithmeticException;
  
  /** If this numeric value is representable as an double value, returns it as an double value. 
   * Otherwise, returns some "close" double value. */
  public abstract double doubleValue();
  
  /** If this numeric value is representable as an double value, returns it as an double value. 
   * Otherwise, throws an {@link ArithmeticException}. */
  public abstract double doubleValueExact();
  
  /** If this numeric value is representable as an 128-bit decimal floating point, returns it as 
   * an 128-bit decimal floating point value. Otherwise, returns some "close" value. */
  public abstract BigDecimal decimalValue();
  
  /** If this numeric value is representable as an 128-bit decimal floating point, returns it as 
   * an 128-bit decimal floating point value. Otherwise throws an {@link ArithmeticException}. */
  public abstract BigDecimal decimalValueExact();



  // -- comparison --------------------------------------------------------------------------------
  
  // longs
  protected static final int compare(JsonLong value1, JsonLong value2)
  {
    long v1 = value1.get();
    long v2 = value2.get();
    return v1<v2 ? -1 : (v1 == v2 ? 0 : +1);
  }
  
  // doubles  
  protected static final int compare(JsonDouble value1, JsonDouble value2)
  {
    double v1 = value1.get();
    double v2 = value2.get();
    return v1<v2 ? -1 : (v1 == v2 ? 0 : +1);
  }
  
  protected static final int compare(JsonLong value1, JsonDouble value2)
  {
    long v1 = value1.get();
    if (Math.abs(v1) < 72057594037927936L) // =2^56; represented exactly as doube
    {
      double v1d = v1;
      double v2d = value2.get();
      return v1d < v2d ? -1 : (v1d == v2d ? 0 : +1);
    } 
    else
    {
      return value1.decimalValue().compareTo(value2.decimalValue());
    }
  }
    
  protected static final int compare(JsonDouble value1, JsonLong value2)
  {
    return -compare(value2, value1);
  }
  
  // decimals
  protected static final int compare(JsonDecimal value1, JsonNumber value2)
  {
    BigDecimal v1 = value1.get();
    BigDecimal v2 = value2.decimalValue();
    return v1.compareTo(v2);
  }
  
  protected static final int compare(JsonNumber value1, JsonDecimal value2)
  {
    return -compare(value2, value1);
  }
}
