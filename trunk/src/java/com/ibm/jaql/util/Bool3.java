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
package com.ibm.jaql.util;

/*
 * 
 * 
 */
public enum Bool3
{
  FALSE(0), UNKNOWN(1), TRUE(3);

  private static Bool3[] byValue = {FALSE, UNKNOWN, UNKNOWN, TRUE};

  private int            value;

  /**
   * @param v
   */
  private Bool3(int v)
  {
    this.value = v;
  }

  /**
   * | F ? T | 00 01 11 x & y ---+------- ----+--------- F | F F F 00 | 00 00 00 ? |
   * F ? ? 01 | 00 01 01 T | F ? T 11 | 00 01 11
   * 
   * @param that
   * @return
   */
  public Bool3 and(Bool3 that)
  {
    return byValue[this.value & that.value];
  }

  /**
   * both(x,y) := if x == y then x else ?
   *  | F ? T | 00 01 11 (x1 & y1)(x0 | y0) ---+------- ----+---------- (x & x) |
   * ((x | y) & 0x1) F | F ? ? 00 | 00 01 01 ? | ? ? ? 01 | 01 01 01 T | ? ? T
   * 11 | 01 01 11
   * 
   * @param that
   * @return
   */
  public Bool3 both(Bool3 that)
  {
    return byValue[(this.value & that.value)
        | ((this.value | that.value) & 0x1)];
  }

  /**
   * | F ? T | 00 01 11 x | y ---+------- ----+--------- F | F ? T 00 | 00 01 11 ? | ? ?
   * T 01 | 01 01 11 T | T T T 11 | 11 11 11
   * 
   * @param that
   * @return
   */
  public Bool3 or(Bool3 that)
  {
    return byValue[this.value | that.value];
  }

  /**
   * x |!x x | !x x ^ 0x3 (ie, ~x on two bits) ---+---- ----+----- F | T 00 | 11 ? | ?
   * 01 | 01 T | F 11 | 00
   * 
   * @param that
   * @return
   */
  public Bool3 not()
  {
    return byValue[this.value ^ 0x3];
  }

  /**
   * @return
   */
  public boolean bool()
  {
    return this == TRUE;
  }

  /**
   * @return
   */
  public boolean always()
  {
    return this == TRUE;
  }

  /**
   * @return
   */
  public boolean never()
  {
    return this == FALSE;
  }

  /**
   * @return
   */
  public boolean maybe()
  {
    return this != FALSE;
  }

  /**
   * @return
   */
  public boolean maybeNot()
  {
    return this != TRUE;
  }

  /**
   * @param test
   * @return
   */
  public static Bool3 valueOf(boolean test)
  {
    return test ? TRUE : FALSE;
  }
};
