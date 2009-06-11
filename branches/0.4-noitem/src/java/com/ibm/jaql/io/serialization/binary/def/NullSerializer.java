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
import com.ibm.jaql.json.type.JsonValue;

public class NullSerializer extends BinaryBasicSerializer<JsonValue>
{
  @Override
  public JsonValue newInstance()
  {
    return null;
  }
  
  @Override
  public int compare(DataInput in1, DataInput in2) throws IOException
  {
    return 0;
  }

  @Override
  public void copy(DataInput in, DataOutput out) throws IOException
  {
  }

  @Override
  public JsonValue read(DataInput in, JsonValue target) throws IOException
  {
    return null;
  }

  @Override
  public void skip(DataInput in) throws IOException
  {
  }

  @Override
  public void write(DataOutput out, JsonValue target) throws IOException
  {
  }
}
