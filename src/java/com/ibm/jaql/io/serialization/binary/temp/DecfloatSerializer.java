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
import java.math.BigDecimal;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.schema.DecfloatSchema;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;

class DecfloatSerializer extends BinaryBasicSerializer<JsonNumber>
{
  private DecfloatSchema schema;
  
  // -- construction ------------------------------------------------------------------------------
  
  public DecfloatSerializer(DecfloatSchema schema)
  {
    this.schema = schema; 
  }
  
  // -- serialization -----------------------------------------------------------------------------

  @Override
  public JsonDecimal newInstance()
  {
    return new JsonDecimal();
  }

  @Override
  public JsonDecimal read(DataInput in, JsonValue target) throws IOException
  {
    // get value
    BigDecimal value = readValue(in);
    
    // return result
    if (target == null || !(target instanceof JsonDecimal)) {
      return new JsonDecimal(value);
    } else {
      JsonDecimal t = (JsonDecimal)target;
      t.set(value);
      return t;
    }
  }

  @Override
  public void write(DataOutput out, JsonNumber value) throws IOException
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
    
    // write
    String str = value.decimalValue().toString();
    out.writeUTF(str);
  }
  
  private BigDecimal readValue(DataInput in) throws IOException
  {
    if (schema.getValue() != null)
    {
      return schema.getValue().get();
    }
    else
    {
      // TODO: need to read and write binary or at least avoid alloc string
      // TODO: need to cache bigdecimal
      String str = in.readUTF();
      return new BigDecimal(str);
    }
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    BigDecimal value1 = readValue(in1);
    BigDecimal value2 = readValue(in2);
    return value1.compareTo(value2);
  }
  // TODO: efficient implementation of compare, skip, and copy
}
