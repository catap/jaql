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
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

/** Default serializer for {@link JsonBinary}. */
public class JsonBinarySerializer extends BinaryBasicSerializer<JsonBinary>
{
  @Override
  public JsonBinary newInstance()
  {
    return new JsonBinary();
  }
  
  @Override
  public JsonBinary read(DataInput in, JsonValue target) throws IOException
  {
    int length = BaseUtil.readVUInt(in);
    byte[] bytes;
    JsonBinary t;
    if (target == null || !(target instanceof JsonBinary)) {
      bytes = new byte[length];
      t = new JsonBinary(bytes);
    } else {
      t = (JsonBinary)target;
      t.ensureCapacity(length);
      bytes = t.getInternalBytes();
    }
    in.readFully(bytes, 0, length);
 
    return t;
  }

  @Override
  public void write(DataOutput out, JsonBinary value) throws IOException
  {
    int length = value.getLength();
    BaseUtil.writeVUInt(out, length);
    out.write(value.getInternalBytes(), 0, length);
  }

  @Override
  public int compare(DataInput in1, DataInput in2) throws IOException
  {
    int l1 = BaseUtil.readVUInt(in1);
    int l2 = BaseUtil.readVUInt(in2);

    int m = Math.min(l1, l2);
    for (int i=0; i<m; i++) {
      int b1 = in1.readByte();
      int b2 = in2.readByte();
      if (b1 != b2) {
        return (b1 & 0xff) - (b2 & 0xff);
      }
    }
        
    return l1 - l2;
  }

  @Override
  public void skip(DataInput in) throws IOException
  {
    int length = BaseUtil.readVUInt(in);
    in.skipBytes(length);
  }
  
  // TODO: efficient implementation of copy

}
