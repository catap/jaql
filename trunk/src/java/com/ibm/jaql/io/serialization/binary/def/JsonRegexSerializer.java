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
import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

class JsonRegexSerializer extends BinaryBasicSerializer<JsonRegex>
{
  BinaryBasicSerializer<JsonString> stringSerializer;
  
  public JsonRegexSerializer(BinaryBasicSerializer<JsonString> stringSerializer) {
    this.stringSerializer = stringSerializer;
  }
  
  @Override
  public JsonRegex newInstance()
  {
    return new JsonRegex();
  }

  @Override
  public JsonRegex read(DataInput in, JsonValue target) throws IOException
  {
    if (target==null || !(target instanceof JsonRegex)) {
      JsonString regex = stringSerializer.read(in, null);
      byte flags = in.readByte();
      return new JsonRegex(regex, flags);
    } else {
      JsonRegex t = (JsonRegex)target;
      JsonString regex = stringSerializer.read(in, t.getInternalRegex());
      byte flags = in.readByte();
      t.set(regex, flags);
      return t;
    }
  }

  @Override
  public void write(DataOutput out, JsonRegex value) throws IOException
  {
    stringSerializer.write(out, value.getInternalRegex());
    out.writeByte(value.getFlags());
  }

  //TODO: efficient implementation of compare, skip, and copy
}
