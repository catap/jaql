package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.io.serialization.text.TextFullSerializer;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;

public class JsonArraySerializer extends TextBasicSerializer<JsonArray>
{
  TextFullSerializer fullSerializer;
  
  public JsonArraySerializer(TextFullSerializer fullSerializer) {
    this.fullSerializer = fullSerializer;
  }
  
  @Override
  public void write(PrintStream out, JsonArray value, int indent) throws IOException
  {
    String sep = "";
    out.print("[");
    
    indent += 2;
    for (JsonValue v: value)
    {
      out.println(sep);
      indent(out, indent);
      fullSerializer.write(out, v, indent);
      sep = ",";
    }
    indent -= 2;
    
    if (sep.length() > 0) // if not empty array
    {
      out.println();
      indent(out, indent);
    }
    out.print("]");
  }
}
