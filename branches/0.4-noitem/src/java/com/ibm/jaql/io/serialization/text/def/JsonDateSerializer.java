package com.ibm.jaql.io.serialization.text.def;

import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonDate;

public class JsonDateSerializer extends TextBasicSerializer<JsonDate>
{
  @Override
  public void write(PrintStream out, JsonDate value, int indent)
  {
    out.print("d'");
    out.print(value.toString());
    out.print("'");
  }
}
