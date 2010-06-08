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
import com.ibm.jaql.json.type.JsonSpan;

public class SpanSerializer extends TextBasicSerializer<JsonSpan>
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
