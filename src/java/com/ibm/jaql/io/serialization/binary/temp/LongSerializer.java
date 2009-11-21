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
import com.ibm.jaql.json.schema.LongSchema;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.util.BaseUtil;

final class LongSerializer extends BinaryBasicSerializer<JsonLong>
{
  private LongSchema schema;
  
  // -- construction ------------------------------------------------------------------------------
  
  public LongSerializer(LongSchema schema)
  {
    this.schema = schema; 
  }
  
  // -- serialization -----------------------------------------------------------------------------

  @Override
  public JsonLong read(DataInput in, JsonValue target) throws IOException
  {
    if (schema.isConstant())
    {
      return schema.getConstant();
    }
    
    long value = readValue(in);
    if (target == null || !(target instanceof MutableJsonLong)) {
      return new MutableJsonLong(value);
    } else {
      MutableJsonLong t = (MutableJsonLong)target;
      t.set(value);
      return t;
    }
  }


  @Override
  public void write(DataOutput out, JsonLong value) throws IOException
  {
    if (!schema.matches(value))
    {
      throw new IllegalArgumentException("value not matched by this serializer");
    }
    
    if (schema.isConstant())
    {
      return;
    }
    
    BaseUtil.writeVSLong(out, value.get());
  }
  
  private long readValue(DataInput in) throws IOException
  {
    return BaseUtil.readVSLong(in);
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    if (schema.isConstant())
    {
      return 0;
    }
    
    long value1 = readValue(in1);
    long value2 = readValue(in2);
    return (value1 < value2) ? -1 : (value1==value2 ? 0 : +1);
  }
  
  // TODO: efficient implementation of skip, and copy
}
