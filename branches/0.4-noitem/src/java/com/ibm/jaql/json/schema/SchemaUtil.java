package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonNumeric;

public class SchemaUtil
{
  public static boolean checkInterval(JsonNumeric start, JsonNumeric end)
  {
    return checkInterval(start, end, null, null);
  }
  
  public static boolean checkInterval(JsonNumeric start, JsonNumeric end, 
      JsonNumeric minStart, JsonNumeric minEnd)
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
