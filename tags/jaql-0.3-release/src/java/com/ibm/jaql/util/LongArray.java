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

/**
 * 
 */
public class LongArray
{
  int    n     = 0;
  long[] items = new long[2];

  /**
   * @param x
   */
  public void add(long x)
  {
    if (n == items.length)
    {
      long[] newItems = new long[n * 2];
      System.arraycopy(items, 0, newItems, 0, n);
      items = newItems;
    }
    items[n] = x;
    n++;
  }

  /**
   * @return
   */
  public long pop()
  {
    return items[--n];
  }

  /**
   * @param k
   */
  public void popN(int k)
  {
    if (k > n) throw new IndexOutOfBoundsException();
    n -= k;
  }

  /**
   * @return
   */
  public long top()
  {
    return items[n - 1];
  }

  /**
   * @return
   */
  public boolean empty()
  {
    return n == 0;
  }

  /**
   * @return
   */
  public int size()
  {
    return n;
  }

  /**
   * @param i
   * @return
   */
  public long get(int i)
  {
    return items[i];
  }

  /**
   * 
   */
  public void clear()
  {
    n = 0;
  }
}
