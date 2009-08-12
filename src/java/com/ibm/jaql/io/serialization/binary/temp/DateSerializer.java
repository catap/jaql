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
import com.ibm.jaql.json.schema.DateSchema;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonDate;
import com.ibm.jaql.util.BaseUtil;

class DateSerializer extends BinaryBasicSerializer<JsonDate>
{
  private DateSchema schema;
  private long offset;
  
  // -- construction ------------------------------------------------------------------------------
  
  public DateSerializer(DateSchema schema)
  {
    this.schema = schema; 
    if (schema.getMin() != null)
    {
      offset = schema.getMin().get();
    }
    else
    {
      offset = 0;
    }
  }
  
  // -- serialization -----------------------------------------------------------------------------

  @Override
  public JsonDate read(DataInput in, JsonValue target) throws IOException
  {
    // get value
    long value = readValue(in);
    
    // return result
    if (target == null || !(target instanceof MutableJsonDate)) {
      return new MutableJsonDate(value);
    } else {
      MutableJsonDate t = (MutableJsonDate)target;
      t.set(value);
      return t;
    }
  }


  @Override
  public void write(DataOutput out, JsonDate value) throws IOException
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
    BaseUtil.writeVSLong(out, value.get()-offset);
  }
  
  private long readValue(DataInput in) throws IOException
  {
    if (schema.getValue() != null)
    {
      return schema.getValue().get();
    }
    else
    {
      return BaseUtil.readVSLong(in)+offset;
    }
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    long value1 = readValue(in1);
    long value2 = readValue(in2);
    return (value1 < value2) ? -1 : (value1==value2 ? 0 : +1);
  }  
}
