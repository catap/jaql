/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
