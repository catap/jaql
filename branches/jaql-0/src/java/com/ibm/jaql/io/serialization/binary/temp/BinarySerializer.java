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
import com.ibm.jaql.json.schema.BinarySchema;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonBinary;
import com.ibm.jaql.util.BaseUtil;

final class BinarySerializer extends BinaryBasicSerializer<JsonBinary>
{
  private BinarySchema schema;
  int length = 0;
  
  // -- construction ------------------------------------------------------------------------------
  
  public BinarySerializer(BinarySchema schema)
  {
    this.schema = schema;
    if (schema.getLength() != null)
    {
      length = (int)schema.getLength().intValueExact();
    }
    else
    {
      length = -1;
    }
  }
  
  // -- serialization -----------------------------------------------------------------------------

  @Override
  public JsonBinary read(DataInput in, JsonValue target) throws IOException
  {
    if (schema.isConstant())
    {
      return schema.getConstant();
    }
    
    // read length
    int length = this.length;
    if (length < 0) // non-constant
    {
      length = BaseUtil.readVUInt(in);
    }

    // create target
    MutableJsonBinary t;
    if (target==null || !(target instanceof MutableJsonBinary)) {
      t = new MutableJsonBinary();
    } else {
      t = (MutableJsonBinary)target;
    }
    t.ensureCapacity(length);
    byte[] bytes = t.get();

    // fill bytes, set and return
    in.readFully(bytes, 0, length);
    t.set(bytes, length);
    return t;
  }

  @Override
  public void write(DataOutput out, JsonBinary value) throws IOException
  {
    if (!schema.matches(value))
    {
      throw new IllegalArgumentException("value not matched by this serializer");
    }

    if (schema.isConstant())
    {
      return;
    }
    
    if (length < 0)
    {
      BaseUtil.writeVUInt(out, value.bytesLength());
    }
    
    value.writeBytes(out);
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    if (schema.isConstant())
    {
      return 0;
    }
    
    // read length
    int l1, l2;
    if (length < 0)
    {
      l1 = BaseUtil.readVUInt(in1);
      l2 = BaseUtil.readVUInt(in2);
    }
    else
    {
      l1 = l2 = length;
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
