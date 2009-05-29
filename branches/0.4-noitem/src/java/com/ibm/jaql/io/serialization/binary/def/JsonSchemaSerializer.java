package com.ibm.jaql.io.serialization.binary.def;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;

/** Binary serializer a Json schema. */ 
// TODO: The current implementation simply makes use of the text representation of the schema.
//       It might be more efficient to use a real binary format, if needed.
public class JsonSchemaSerializer extends BinaryBasicSerializer<JsonSchema> 
{
  @Override
  public JsonSchema newInstance()
  {
    return new JsonSchema();
  }

  @Override
  public JsonSchema read(DataInput in, JsonValue target) throws IOException
  {
    String s = in.readUTF();
    Schema schema = Schema.parse(s);
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
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream pout = new PrintStream(bout);
    com.ibm.jaql.io.serialization.text.def.JsonSchemaSerializer.write(pout, value.getSchema(), 0);
    pout.flush();
    String s = bout.toString();
    out.writeUTF(s);
  }
}
