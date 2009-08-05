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
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

class JsonStringSerializer extends BinaryBasicSerializer<JsonString>
{
  @Override
  public JsonString newInstance()
  {
    return new JsonString();
  }

  @Override
  public JsonString read(DataInput in, JsonValue target) throws IOException
  {
    int length = BaseUtil.readVUInt(in);
    
    JsonString t;
    byte[] bytes;
    if (target==null || !(target instanceof JsonString)) {
      bytes = new byte[length];
      t = new JsonString(bytes);
    } else {
      t = (JsonString)target;
      bytes = t.getInternalBytes();
      if (bytes.length < length) {
        bytes = new byte[length];
      }
    }
    in.readFully(bytes, 0, length);
    t.set(bytes, length);
    return t;
  }

  @Override
  public void write(DataOutput out, JsonString value) throws IOException
  {
    int length = value.lengthUtf8();
    BaseUtil.writeVUInt(out, length);
    out.write(value.getInternalBytes(), 0, length);
    
  }

  //TODO: efficient implementation of compare, skip, and copy
}
