package com.ibm.jaql.io.hadoop;

import com.ibm.jaql.json.type.JsonValue;

public final class JsonHolder
{
  public JsonValue value;
  
  public JsonHolder()
  {
  };
  
  public JsonHolder(JsonValue value) 
  {
    this.value = value;
  }
}
