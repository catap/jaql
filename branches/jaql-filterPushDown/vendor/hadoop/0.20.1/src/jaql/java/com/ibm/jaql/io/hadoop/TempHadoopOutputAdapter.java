package com.ibm.jaql.io.hadoop;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;


public class TempHadoopOutputAdapter<K,V> extends DefaultHadoopOutputAdapter<K,V>
{
  @Override
  protected void init(JsonRecord args) throws Exception
  {
    super.init(args);
    
    // check for schema option
    JsonValue v = options.get(new JsonString("schema"));
    if (!(v instanceof JsonSchema))
    {
      throw new IllegalArgumentException("\"schema\" option not present or of invalid type");
    }
  }
  
  
  @Override
  protected JsonHolder keyHolder()
  {
    return new JsonHolderTempKey();
  }

  @Override
  protected JsonHolder valueHolder()
  {
    return new JsonHolderTempValue();
  }
}
