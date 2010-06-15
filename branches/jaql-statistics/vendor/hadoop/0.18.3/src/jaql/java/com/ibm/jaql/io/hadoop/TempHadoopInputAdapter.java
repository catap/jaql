package com.ibm.jaql.io.hadoop;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;

public class TempHadoopInputAdapter<K,V> extends DefaultHadoopInputAdapter<K,V>
{
  Schema schema;
  
  @Override
  protected void initializeFrom(JsonRecord args) throws Exception
  {
    super.initializeFrom(args);
    
    // check for schema option
    JsonSchema v = (JsonSchema)options.get(new JsonString("schema"));
    if (!(v instanceof JsonSchema))
    {
      throw new IllegalArgumentException("\"schema\" option not present or of invalid type");
    }
    schema = v.get();
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
  
  @Override
  public Schema getSchema()
  {
      return new ArraySchema(null, schema);
  }
}
