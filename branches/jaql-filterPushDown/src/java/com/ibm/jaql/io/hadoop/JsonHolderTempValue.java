package com.ibm.jaql.io.hadoop;

import com.ibm.jaql.json.type.JsonValue;

/** Used to write in Jaql's temp format. */
public class JsonHolderTempValue extends JsonHolder
{
  public JsonHolderTempValue()
  {
    super();
  };
  
  public JsonHolderTempValue(JsonValue value) 
  {
    super(value);
  }
}
