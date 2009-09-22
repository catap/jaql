package com.ibm.jaql.io.hadoop;

import com.ibm.jaql.json.type.JsonValue;

/** Holds a JSON value. Mainly used for storing JSON values in SequenceFiles */
public class JsonHolderDefault extends JsonHolder
{
  public JsonHolderDefault()
  {
    super();
  };
  
  public JsonHolderDefault(JsonValue value) 
  {
    super(value);
  }
}
