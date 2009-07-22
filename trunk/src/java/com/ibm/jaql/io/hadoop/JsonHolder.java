package com.ibm.jaql.io.hadoop;

import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;

/** Holds a JSON value. Mainly used for storing JSON values in SequenceFiles */
public class JsonHolder
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
    int hashCode = JsonUtil.hashCode(value);
    return hashCode;
  }
}
