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
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonDouble;

class JsonDoubleSerializer extends BinaryBasicSerializer<JsonDouble>
{
  @Override
  public JsonDouble read(DataInput in, JsonValue target) throws IOException
  {
    double value = in.readDouble();
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
    out.writeDouble(value.get());
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
