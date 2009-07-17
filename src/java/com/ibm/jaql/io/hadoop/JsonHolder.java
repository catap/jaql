package com.ibm.jaql.io.hadoop;

import com.ibm.jaql.json.type.JsonUtil;
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
  
  @Override
  public int hashCode()
  {
    return JsonUtil.hashCode(value);
  }
}
