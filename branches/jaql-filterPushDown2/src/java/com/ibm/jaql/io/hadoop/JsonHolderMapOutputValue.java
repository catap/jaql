package com.ibm.jaql.io.hadoop;

import com.ibm.jaql.json.type.JsonValue;

/** Used for map output values when there is an reducer. */
public final class JsonHolderMapOutputValue extends JsonHolder
{
  public JsonHolderMapOutputValue()
  {
    super();
  };
  
  public JsonHolderMapOutputValue(JsonValue value) 
  {
    super(value);
  }
}
