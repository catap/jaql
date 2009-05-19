package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonSchema;

public class JsonSchemaSerializer extends TextBasicSerializer<JsonSchema>
{

  @Override
  public void write(PrintStream out, JsonSchema value, int indent)
      throws IOException
  {
    out.print("type ");
    out.print(value.getSchema().toString());    
  }

}
