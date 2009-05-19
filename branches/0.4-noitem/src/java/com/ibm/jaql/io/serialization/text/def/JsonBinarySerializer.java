package com.ibm.jaql.io.serialization.text.def;

import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.util.BaseUtil;

public class JsonBinarySerializer extends TextBasicSerializer<JsonBinary>
{
  @Override
  public void write(PrintStream out, JsonBinary value, int indent)
  {
    out.print("x'");
    byte[] v = value.getInternalBytes();
    for (int i = 0; i < v.length ; i++)
    {
      byte b = v[i];
      out.print(BaseUtil.HEX_NIBBLE[(b >> 4) & 0x0f]);
      out.print(BaseUtil.HEX_NIBBLE[b & 0x0f]);
    }
    out.print('\'');
  }
}
