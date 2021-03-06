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
package com.ibm.jaql.io.serialization.binary.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonValue;

class JsonBoolSerializer extends BinaryBasicSerializer<JsonBool>
{
  @Override
  public JsonBool read(DataInput in, JsonValue target) throws IOException
  {
    boolean value = in.readByte() != 0; 
    return JsonBool.make(value);
  }

  @Override
  public void write(DataOutput out, JsonBool value) throws IOException
  {
    out.writeByte(value.get() ? 1 : 0);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
