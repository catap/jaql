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
