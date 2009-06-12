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

import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonValue;

/** Helper methods used internally */
class SchemaUtil
{
  /** Checks whether start <= end */
  public static boolean checkInterval(JsonValue start, JsonValue end)
  {
    return checkInterval(start, end, null, null);
  }
  
  /** Checks whether minStart <= start <= end <= minEnd */
  public static boolean checkInterval(JsonValue start, JsonValue end, 
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
      return JsonNumeric.compare(start, end) <= 0; 
    }
    return true;    
  }

}
