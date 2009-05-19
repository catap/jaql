package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonDouble;

public class JsonDoubleSerializer extends TextBasicSerializer<JsonDouble>
{

  @Override
  public void write(PrintStream out, JsonDouble value, int indent)
      throws IOException
  {
    out.print(value.value);
    out.print('d'); // TODO: flag to disable suffix for JSON compatibility
  }
}
