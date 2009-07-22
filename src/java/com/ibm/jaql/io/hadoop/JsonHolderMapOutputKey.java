package com.ibm.jaql.io.hadoop;

import com.ibm.jaql.json.type.JsonValue;

/** Used for map output keys when there is an reducer. */
public final class JsonHolderMapOutputKey extends JsonHolder
{
  public JsonHolderMapOutputKey()
  {
    super();
  };
  
  public JsonHolderMapOutputKey(JsonValue value) 
  {
    super(value);
  }
}
