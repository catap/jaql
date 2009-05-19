package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.io.serialization.text.TextFullSerializer;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;

public class JsonRecordSerializer extends TextBasicSerializer<JsonRecord>
{
  TextBasicSerializer<JsonString> nameSerializer;
  TextFullSerializer valueSerializer;

  public JsonRecordSerializer(TextBasicSerializer<JsonString> nameSerializer, TextFullSerializer valueSerializer)
  {
    this.nameSerializer = nameSerializer ;
    this.valueSerializer = valueSerializer;
  }
  
  
  @Override
  public void write(PrintStream out, JsonRecord value, int indent)
      throws IOException
  {
    out.print("{");
    
    indent += 2;
    final int arity = value.arity();
    String sep = "";
    for (int i = 0; i < arity; i++)
    {
      out.println(sep);
      indent(out, indent);
      nameSerializer.write(out, value.getName(i), indent);
      out.print(": ");
      valueSerializer.write(out, value.getValue(i), indent);
      sep = ",";
    }
    indent -= 2;
    
    if (sep.length() > 0) // if not empty record
    {
      out.println();
      indent(out, indent);
    }
    out.print("}");  
  }
}
