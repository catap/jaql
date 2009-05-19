package com.ibm.jaql.io.serialization.text.def;

import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonBool;

public class JsonBoolSerializer extends TextBasicSerializer<JsonBool>
{
  @Override
  public void write(PrintStream out, JsonBool value, int indent)
  {
    out.print(value.getValue() ? "true" : "false");
  }
}
