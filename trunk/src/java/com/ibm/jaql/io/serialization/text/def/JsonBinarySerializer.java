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

import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.util.BaseUtil;

public class JsonBinarySerializer extends TextBasicSerializer<JsonBinary>
{
  @Override
  public void write(PrintStream out, JsonBinary value, int indent)
  {
    out.print("hex('");
    for (int i = 0; i < value.bytesLength() ; i++)
    {
      byte b = value.get(i);
      out.print(BaseUtil.HEX_NIBBLE[(b >> 4) & 0x0f]);
      out.print(BaseUtil.HEX_NIBBLE[b & 0x0f]);
    }
    out.print("')");
  }
}
