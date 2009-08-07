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
import java.util.Iterator;
import java.util.Map.Entry;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.util.BaseUtil;

class BufferedJsonRecordSerializer extends BinaryBasicSerializer<BufferedJsonRecord>
{
  BinaryBasicSerializer<JsonString> nameSerializer;
  BinaryFullSerializer valueSerializer;

  public BufferedJsonRecordSerializer(BinaryBasicSerializer<JsonString> nameSerializer, BinaryFullSerializer valueSerializer)
  {
    this.nameSerializer = nameSerializer ;
    this.valueSerializer = valueSerializer;
  }

  
  @Override
  public BufferedJsonRecord newInstance()
  {
    return new BufferedJsonRecord();
  }


  @Override
  public BufferedJsonRecord read(DataInput in, JsonValue target) throws IOException
  {
    int arity = BaseUtil.readVUInt(in);
    BufferedJsonRecord t;
    if (target==null || !(target instanceof BufferedJsonRecord)) {
      t = new BufferedJsonRecord(arity);
    } else {
      t = (BufferedJsonRecord)target;
      t.ensureCapacity(arity);
    }

    JsonString[] names = t.getInternalNamesArray();
    JsonValue[] values = t.getInternalValuesArray();
    for (int i = 0; i < arity; i++)
    {
      names[i] = nameSerializer.read(in, names[i]);
      values[i] = valueSerializer.read(in, values[i]);
    }
    
    t.setInternal(names, values, arity, true);    
    return t;
  }


  @Override
  public void write(DataOutput out, BufferedJsonRecord value) throws IOException
  {
    int arity = value.size();
    BaseUtil.writeVUInt(out, arity);
    Iterator<Entry<JsonString, JsonValue>> it = value.iteratorSorted(); // write in sorted order for comparison!
    while (it.hasNext())
    {
      Entry<JsonString, JsonValue> entry = it.next();
      nameSerializer.write(out, entry.getKey());
      valueSerializer.write(out, entry.getValue());
    }
  }
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    int arity1 = BaseUtil.readVUInt(in1);
    int arity2 = BaseUtil.readVUInt(in2);
    int m = Math.min(arity1, arity2);
    for (int i=0; i<m; i++) {
      // names can be overwritten; they are only used here
      int cmp = nameSerializer.compare(in1, in2);
      if (cmp != 0) return cmp;
        
      // compare the values
      cmp = valueSerializer.compare(in1, in2);
      if (cmp != 0) return cmp;
    }
    return arity1-arity2;
  }

  //TODO: efficient implementation of skip, and copy
}

