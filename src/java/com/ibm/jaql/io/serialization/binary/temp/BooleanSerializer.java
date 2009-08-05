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
import com.ibm.jaql.json.schema.BooleanSchema;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonValue;

class BooleanSerializer extends BinaryBasicSerializer<JsonBool>
{
  private BooleanSchema schema;
  
  // -- construction ------------------------------------------------------------------------------
  
  public BooleanSerializer(BooleanSchema schema)
  {
    this.schema = schema; 
  }

  // -- serialization -----------------------------------------------------------------------------

  @Override
  public JsonBool newInstance()
  {
    return new JsonBool();
  }

  @Override
  public JsonBool read(DataInput in, JsonValue target) throws IOException
  {
    // obtain value
    boolean value;
    if (schema.getValue() != null)
    {
      value = schema.getValue().get(); // copy!
    }
    else
    {
      value = in.readByte() != 0;
    }
    
    // return it 
    if (target == null || !(target instanceof JsonBool)) {
      return new JsonBool(value);
    } else {
      JsonBool t = (JsonBool)target;
      t.set(value);
      return t;
    }
  }

  @Override
  public void write(DataOutput out, JsonBool value) throws IOException
  {
    // check match
    if (!schema.matches(value))
    {
      throw new IllegalArgumentException("value not matched by this serializer");
    }

    // write
    if (schema.getValue() != null)
    {
      // constant
      return;
    }
    else
    {
      out.writeByte(value.get() ? 1 : 0);
    }
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    if (schema.getValue() != null)
    {
      return 0;
    }

    // obtain value
    boolean value1 = in1.readByte() != 0;
    boolean value2 = in2.readByte() != 0;
    return (value1 == value2) ? 0 : (value2 ? -1 : +1);
  }

  //TODO: efficient implementation of skip, and copy
}
