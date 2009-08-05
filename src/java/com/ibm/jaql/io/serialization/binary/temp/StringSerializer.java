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
package com.ibm.jaql.io.serialization.binary.temp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.schema.StringSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

class StringSerializer extends BinaryBasicSerializer<JsonString>
{
  private StringSchema schema;
  int minLength = 0;
  boolean constantLength = false;
  // -- construction ------------------------------------------------------------------------------
  
  public StringSerializer(StringSchema schema)
  {
    this.schema = schema;
    if (schema.getValue() != null)
    {
      constantLength = true;
      minLength = schema.getValue().lengthUtf8();
    }
    else if (schema.getMinLength() != null)
    {
      minLength = schema.getMinLength().intValueExact();
      constantLength = JsonUtil.equals(schema.getMinLength(), schema.getMaxLength());  
    }
  }
  
  // -- serialization -----------------------------------------------------------------------------

  @Override
  public JsonString newInstance()
  {
    return new JsonString();
  }

  @Override
  public JsonString read(DataInput in, JsonValue target) throws IOException
  {
    // read length
    int length;
    if (constantLength)
    {
      length = minLength;
    }
    else
    {
      length = BaseUtil.readVUInt(in)+minLength;
    }

    // create target
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

    // fill bytes
    if (schema.getValue() == null)
    {
      in.readFully(bytes, 0, length);
    }
    else
    {
      System.arraycopy(schema.getValue().getInternalBytes(), 0, bytes, 0, length); 
    }
    
    
    // set and return
    t.set(bytes, length);
    return t;
  }


  @Override
  public void write(DataOutput out, JsonString value) throws IOException
  {
    // check match
    if (!schema.matches(value))
    {
      throw new IllegalArgumentException("value not matched by this serializer");
    }
    
    // check constant
    if (schema.getValue() != null)
    {
      return;
    }
    
    // write length
    int length = value.lengthUtf8();
    if (!constantLength)
    {
      BaseUtil.writeVUInt(out, length-minLength);
    }
    
    // write
    out.write(value.getInternalBytes(), 0, length);
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    // read length
    int l1, l2;
    if (constantLength)
    {
      l1 = l2 = minLength;
    }
    else
    {
      l1 = BaseUtil.readVUInt(in1)+minLength;
      l2 = BaseUtil.readVUInt(in2)+minLength;
    }

    // now compare bytes
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

  // TODO: efficient implementation of skip, and copy
}
