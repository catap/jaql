package com.ibm.jaql.io.hadoop;

import com.ibm.jaql.json.type.JsonValue;

/** Used to write in Jaql's temp format. */
public class JsonHolderTempKey extends JsonHolder
{
  public JsonHolderTempKey()
  {
    super();
  };
  
  public JsonHolderTempKey(JsonValue value) 
  {
    super(value);
  }
}
