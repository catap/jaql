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
import com.ibm.jaql.json.schema.DoubleSchema;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonDouble;

final class DoubleSerializer extends BinaryBasicSerializer<JsonDouble>
{
  private DoubleSchema schema;
  
  // -- construction ------------------------------------------------------------------------------
  
  public DoubleSerializer(DoubleSchema schema)
  {
    this.schema = schema; 
  }
  
  // -- serialization -----------------------------------------------------------------------------

  @Override
  public JsonDouble read(DataInput in, JsonValue target) throws IOException
  {
    if (schema.isConstant())
    {
      return schema.getConstant();
    }
    
    double value = readValue(in);
    if (target == null || !(target instanceof MutableJsonDouble)) {
      return new MutableJsonDouble(value);
    } else {
      MutableJsonDouble t = (MutableJsonDouble)target;
      t.set(value);
      return t;
    }
  }


  @Override
  public void write(DataOutput out, JsonDouble value) throws IOException
  {
    if (!schema.matches(value))
    {
      throw new IllegalArgumentException("value not matched by this serializer");
    }
    
    if (schema.isConstant())
    {
      return;
    }
    
    out.writeDouble(value.get());
  }
  
  
  private double readValue(DataInput in) throws IOException
  {
    return in.readDouble();
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    if (schema.isConstant())
    {
      return 0;
    }
    
    double value1 = readValue(in1);
    double value2 = readValue(in2);
    return (value1 < value2) ? -1 : (value1==value2 ? 0 : +1);
  }
  
  // TODO: efficient implementation of skip, and copy
}
