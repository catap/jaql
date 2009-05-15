package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;

public class JsonSchemaSerializer extends BasicSerializer<JsonSchema> 
{

  @Override
  public JsonSchema newInstance()
  {
    return new JsonSchema();
  }

  @Override
  public JsonSchema read(DataInput in, JsonValue target) throws IOException
  {
    Schema schema = Schema.read(in);
    if (target == null || !(target instanceof JsonSchema)) {
      return new JsonSchema(schema);
    } else {
      JsonSchema t = (JsonSchema)target;
      t.setSchema(schema);
      return t;
    }
  }

  @Override
  public void write(DataOutput out, JsonSchema value) throws IOException
  {
    value.getSchema().write(out);
  }
}
