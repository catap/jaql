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
package com.acme.extensions.fn;

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;

/**
 * 
 */
public class GCD2
{
  /**
   * @param a
   * @param b
   * @return
   */
  private long gcd(long a, long b)
  {
    while (b != 0)
    {
      long c = b;
      b = a % b;
      a = c;
    }
    return a;
  }

  /**
   * @param x
   * @param y
   * @return
   */
  public JsonLong eval(JsonNumber x, JsonNumber y)
  {
    long a = x.longValueExact();
    long b = y.longValueExact();
    long g = gcd(a, b);
    return new JsonLong(g);
  }
}
