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
 * The base of all numeric types. Not all numeric types are the jaql type
 * "number", and therefore are not derived from JNumber (eg. JDouble).
 */
public abstract class JNumeric extends JAtom
{
  /**
   * @return
   */
  public abstract int intValue();
  /**
   * @return
   */
  public abstract int intValueExact();
  /**
   * @return
   */
  public abstract long longValue();
  /**
   * @return
   */
  public abstract long longValueExact();
  /**
   * @return
   */
  public abstract BigDecimal decimalValue();
  /**
   * @return
   */
  public abstract double doubleValue();
  /**
   * 
   */
  public abstract void negate();
}
