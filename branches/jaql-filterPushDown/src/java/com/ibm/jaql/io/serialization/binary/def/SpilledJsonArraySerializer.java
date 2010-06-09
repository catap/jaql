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

import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.util.BaseUtil;

class SpilledJsonArraySerializer extends BinaryBasicSerializer<SpilledJsonArray>
{
  BinaryFullSerializer fullSerializer;

  public SpilledJsonArraySerializer(BinaryFullSerializer fullSerializer)
  {
    this.fullSerializer = fullSerializer;
  }

  @Override
  public SpilledJsonArray read(DataInput in, JsonValue target) throws IOException
  {
    SpilledJsonArray t;
    if (target==null || !(target instanceof SpilledJsonArray)) {
      t = new SpilledJsonArray();
    } else {
      t = (SpilledJsonArray)target;
    }

    t.clear();
    long count = BaseUtil.readVULong(in);
    for (long i = 0; i < count; i++)
    {
      t.addCopySerialized(in, fullSerializer);
    }
    assert t.count() == count;

    t.freeze();
    return t;
  }

  @Override
  public void write(DataOutput out, SpilledJsonArray v) throws IOException
  {
    // update AscDescItemComparator when changing this
    
    v.freeze();
    BaseUtil.writeVULong(out, v.count());
    
    // write cached items
    int m = v.count() < v.getCacheSize() ? (int)v.count() : v.getCacheSize();
    JsonValue[] cache = v.getInternalCache();
    for (int i=0; i<m; i++) {
      fullSerializer.write(out, cache[i]);
    }
    
    // write spilled items
    if (v.hasSpillFile()) {
      assert fullSerializer.equals(v.getSpillSerializer()); // currently trivially true
      v.copySpillFile(out);
    }    
  }

  public int compare(DataInput in1, DataInput in2) throws IOException {
    long n1 = BaseUtil.readVULong(in1);
    long n2 = BaseUtil.readVULong(in2);
    return FullSerializer.compareArrays(in1, n1, in2, n2, fullSerializer);
  }

  // TODO: efficient implementation of skip, and copy
}
