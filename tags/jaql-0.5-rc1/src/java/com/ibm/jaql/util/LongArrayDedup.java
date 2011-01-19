/*
 * Copyright (C) IBM Corp. 2009.
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


/** Dynamically growing array of distinct longs.
 *  The values are deduped periodically:
 *     when expansion is required,
 *     when sort is called,
 *     when trimToSize is called
 *  You must call sort() or trimToSize() after all insertions to ensure
 *  duplicates are removed before using indexOf().
 */
public class LongArrayDedup extends LongArray
{
  private long max;

  public LongArrayDedup()
  {
  }
  
  public LongArrayDedup(int initialCapacity)
  {
    super(initialCapacity);
  }
  
  /**
   * Add x to the array.  Duplicate values are sometimes eliminated during this insert.
   * When inserting sorted items, even with duplicates, we never need to sort.
   * When inserting grouped items (all dups consecutive), we sort, but never waste any space on duplicates.
   */
  @Override
  public void add(long x)
  {
    if (n == items.length)
    {
      sort(); // attempt to dedup to make room without reallocating 
      if (n == items.length) // no dups, so we have to grow
      {
        grow();
      }
    }
    if( x > max || n == 0 ) // We only need n == 0 for the sad case when the first item is Long.MIN_VALUE
    {
      items[n] = max = x;
      n++;
      // assert sorted == true;
    }
    else if( items[n-1] != x) // we never insert duplicates right next to each other
    {
      items[n] = x;
      n++;
      sorted = false;
    }
  }

  @Override
  public void clear()
  {
    super.clear();
    sorted = true;
    max = Long.MIN_VALUE;
  }
  
  @Override
  public void trimToSize()
  {
    sort();
    super.trimToSize();
  }
  
  @Override
  public void sort()
  {
    if( !sorted )
    {
      super.sort();
      // dedup
      int i = 1;
      while( i < n )
      {
        if( items[i-1] == items[i] )
        {
          // assert i + 1 == n || items[i] != items[i+1]
          System.arraycopy(items, i, items, i-1, n-i-1);
          n--;
        }
        i++; // we can always advance because items[i] != items[i+1] 
      }
    }
  }
}
