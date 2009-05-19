package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.util.JsonUtil;

public class JsonStringSerializer extends TextBasicSerializer<JsonString>
{

  @Override
  public void write(PrintStream out, JsonString value, int indent)
      throws IOException
  {
    String s = value.toString();
    JsonUtil.printQuoted(out, s);
  }
}
