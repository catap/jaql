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

/** The base of all numeric types. Not all numeric types are also of type number and therefore 
 * are not derived from {@link JsonNumber} (eg. {@link JsonDouble}. */
public abstract class JsonNumeric extends JsonAtom
{
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
  
  /** Returns this numeric value as 128-bit decimal floating point number. */
  public abstract BigDecimal decimalValue();
  
  /** If this numeric value is representable as an double value, returns it as an double value. 
   * Otherwise, returns some "close" double value. */
  public abstract double doubleValue();
}
