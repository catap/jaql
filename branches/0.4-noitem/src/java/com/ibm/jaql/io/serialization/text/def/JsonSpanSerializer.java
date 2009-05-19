package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonSpan;

public class JsonSpanSerializer extends TextBasicSerializer<JsonSpan>
{
  @Override
  public void write(PrintStream out, JsonSpan value, int indent)
      throws IOException
  {
    out.print("span(");
    out.print(value.begin);
    out.print(',');
    out.print(value.end);
    out.print(')');
  }
}
