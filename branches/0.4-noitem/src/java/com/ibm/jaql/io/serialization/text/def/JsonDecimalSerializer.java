package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonDecimal;

public class JsonDecimalSerializer extends TextBasicSerializer<JsonDecimal>
{

  @Override
  public void write(PrintStream out, JsonDecimal value, int indent)
      throws IOException
  {
    out.print(value.value.toString());
  }
}
