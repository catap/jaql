package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonLong;

public class JsonLongSerializer extends TextBasicSerializer<JsonLong>
{

  @Override
  public void write(PrintStream out, JsonLong value, int indent)
      throws IOException
  {
    out.print(value.value);
  }
}
