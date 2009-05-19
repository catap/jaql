package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.lang.core.JaqlFunction;

public class JaqlFunctionSerializer extends TextBasicSerializer<JaqlFunction>
{

  @Override
  public void write(PrintStream out, JaqlFunction value, int indent)
      throws IOException
  {
    out.print(value.getText());
  }
}
