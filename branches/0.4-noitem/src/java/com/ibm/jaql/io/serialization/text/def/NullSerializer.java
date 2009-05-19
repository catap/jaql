package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonValue;

public class NullSerializer extends TextBasicSerializer<JsonValue>
{
  @Override
  public void write(PrintStream out, JsonValue value, int indent)
      throws IOException
  {
    out.print("null");    
  }
}
