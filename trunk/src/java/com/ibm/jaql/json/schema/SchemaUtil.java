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

/** Package-private helper methods */
class SchemaUtil
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
  public static <T extends JsonValue> T min(T v1, T v2)
  {
    if (v1 == null)
    {
      return null;
    } 
    else if (v2 == null)
    {
      return null;  
    }
    else
    {
      return v1.compareTo(v2) <= 0 ? v1 : v2;
    }
  }
  
  /** Returns the minimum value of its arguments or null if one of its inputs is null  */
  public static <T extends JsonValue> T min(T v1, T v2, T v3)
  {
    return min(min(v1, v2), v3);
  }

  /** Returns the minimum value of its arguments or null if one of its inputs is null  */
  public static <T extends JsonValue> T min(T v1, T v2, T v3, T v4)
  {
    return min(min(v1, v2), min(v3, v4));
  }

  /** Returns the maximum value of its arguments or null if one of its inputs is null  */
  public static <T extends JsonValue> T max(T v1, T v2)
  {
    if (v1 == null)
    {
      return null;
    } 
    else if (v2 == null)
    {
      return null;  
    }
    else
    {
      return v1.compareTo(v2) >= 0 ? v1 : v2;
    }
  }
  
  /** Returns the maximum value of its arguments or null if one of its inputs is null  */
  public static <T extends JsonValue> T max(T v1, T v2, T v3)
  {
    return max(max(v1, v2), v3);
  }

  /** Returns the maximum value of its arguments or null if one of its inputs is null  */
  public static <T extends JsonValue> T max(T v1, T v2, T v3, T v4)
  {
    return max(max(v1, v2), max(v3, v4));
  }
  
  public static <T extends JsonValue> T minOrValue(T min1, T min2, T v1, T v2)
  {
    if (v1!=null) min1=v1;
    if (v2!=null) min2=v2;
    if (v1==null) v1=min1;
    if (v2==null) v2=min2;
    return min(min1, min2, v1, v2);
  }
  
  public static <T extends JsonValue> T maxOrValue(T max1, T max2, T v1, T v2)
  {
    if (v1!=null) max1=v1;
    if (v2!=null) max2=v2;
    if (v1==null) v1=max1;
    if (v2==null) v2=max2;
    return max(max1, max2, v1, v2);
  }


}
