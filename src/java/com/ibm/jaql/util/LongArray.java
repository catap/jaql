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

import java.util.Arrays;

/** Dynamically growing array of longs. */
public class LongArray
{
  protected static final int DEFAULT_CAPACITY = 8;
  
  int    n     = 0;
  long[] items;
  boolean sorted = true;

  public LongArray()
  {
    this(DEFAULT_CAPACITY);
  }
  
  public LongArray(int initialCapacity)
  {
    this.items = new long[initialCapacity];
  }
  
  /**
   * @param x
   */
  public void add(long x)
  {
    if (n == items.length)
    {
      grow();
    }
    // TODO: move add-in-order detection (from LongArrayDedup) here to avoid unneeded sort?
    items[n] = x;
    n++;
    sorted = false;
  }
  
  /** Make the items array at least one bigger */
  protected void grow()
  {
    long[] newItems = new long[n < Integer.MAX_VALUE / 2 ? n * 2 : Integer.MAX_VALUE - 1];
    System.arraycopy(items, 0, newItems, 0, n);
    items = newItems;
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
    sorted = true;
  }

  /** Shrink the allocated array if we are being wasteful */
  public void trimToSize()
  {
    if( items.length > DEFAULT_CAPACITY && n < .95 * items.length )
    {
      long[] newItems = new long[n > 2 ? n : 2];
      System.arraycopy(items, 0, newItems, 0, n);
      items = newItems;
    }
  }
  
  /** Sort the elements in ascending order */
  public void sort()
  {
    if( !sorted )
    {
      Arrays.sort(items, 0, n); // write a faster sort, eg radix-sort?
      sorted = true;
    }
  }

  /** Has the array been sorted? */
  public boolean isSorted()
  {
    return sorted;
  }
  
  /** 
   * Return the index of the item if it is in the array, otherwise a value < 0.
   * If the array is sorted, it returns (-(insertion point) - 1), as defined by Arrays.binarySearch(). 
   */
  public int indexOf(long x)
  {
    if( sorted )
    {
      return Arrays.binarySearch(items, 0, n, x);
    }
    else
    {
      for(int i = 0 ; i < n ; i++)
      {
        if( items[i] == x )
        {
          return i;
        }
      }
      return -1;
    }
  }
}
