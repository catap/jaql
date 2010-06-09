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
package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;

/** helper methods */
public class SchemaUtil
{
  /** Checks whether start <= end */
  static boolean checkInterval(JsonValue start, JsonValue end)
  {
    return checkInterval(start, end, null, null);
  }
  
  /** Checks whether minStart <= start <= end <= minEnd */
  static boolean checkInterval(JsonValue start, JsonValue end, 
      JsonValue minStart, JsonValue minEnd)
  {
    if (start==null && end==null) 
    {
      return true;
    }
    
    boolean startOk = start==null || minStart==null || start.compareTo(minStart) >= 0;
    if (!startOk) return false;
    boolean endOk = end==null || minEnd==null || end.compareTo(minEnd) >= 0;
    if (!endOk) return false;
    
    if (start!=null && end!=null) 
    {
      return JsonUtil.compare(start, end) <= 0; 
    }
    return true;    
  }
  
  /** Returns the minimum value of its arguments or null if one of its inputs is null  */
  public static <T extends JsonValue> T min(T v, T ... values)
  {
    if (v == null)
    {
      return null;
    }
    
    T min = v;
    for (T next : values)
    {
      if (next == null)
      {
        return null;  
      }
      min = min.compareTo(next) <= 0 ? min : next;
    }    
    return min;
  }
  
    /** Returns the maximum value of its arguments or null if one of its inputs is null  */
  public static <T extends JsonValue> T max(T v, T ... values)
  {
    if (v == null)
    {
      return null;
    }
    
    T max = v;
    for (T next : values)
    {
      if (next == null)
      {
        return null;  
      }
      max = max.compareTo(next) >= 0 ? max : next;
    }    
    return max;
  }
  
  @SuppressWarnings("unchecked")
  public static <T extends JsonValue> T minOrValue(T min1, T min2, T v1, T v2)
  {
    if (v1!=null) min1=v1;
    if (v2!=null) min2=v2;
    if (v1==null) v1=min1;
    if (v2==null) v2=min2;
    return min(min1, min2, v1, v2);
  }
  
  @SuppressWarnings("unchecked")
  public static <T extends JsonValue> T maxOrValue(T max1, T max2, T v1, T v2)
  {
    if (v1!=null) max1=v1;
    if (v2!=null) max2=v2;
    if (v1==null) v1=max1;
    if (v2==null) v2=max2;
    return max(max1, max2, v1, v2);
  }

  /** Compares two arrays, element by element. Handles null arguments (nulls go last) and null 
   * values in the provided arrays gracefully (nulls go first). */
  @SuppressWarnings("unchecked")
  public static <T extends Comparable> int arrayCompare(T[] a1, T[] a2)
  {
    if (a1 == null && a2 == null) return 0;
    if (a1 != null && a2 == null) return -1;
    if (a1 == null && a2 != null) return +1;
    
    // both non-null
    int l1 = a1.length;
    int l2 = a2.length;
    int l = Math.min(l1, l2);
    for (int i=0; i<l; i++)
    {
      if (a1[i] == null && a2[i] == null) continue;
      if (a1[i] != null && a2[i] == null) return +1;
      if (a1[i] == null && a2[i] != null) return -1;
      
      int cmp = a1[i].compareTo(a2[i]);
      if (cmp != 0) return cmp;
    }
    
    // both have same prefix; length decides
    return l1 < l2 ? -1 : (l1==l2) ? 0 : +1; 
  }  
  
  /** Handles null (nulls go last) */
  @SuppressWarnings("unchecked")
  public static <T extends Comparable> int compare(T v1, T v2) {
    if (v1 == null) {
      return v2==null ? 0 : +1; // nulls go first
    } 
    if (v2 == null) {
      return -1;
    }
    return v1.compareTo(v2);
  }
}
