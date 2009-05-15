package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.util.BaseUtil;

public class SpilledJsonArraySerializer extends BasicSerializer<SpilledJsonArray>
{
  FullSerializer fullSerializer;

  public SpilledJsonArraySerializer(FullSerializer fullSerializer)
  {
    this.fullSerializer = fullSerializer;
  }

  @Override
  public SpilledJsonArray newInstance()
  {
    return new SpilledJsonArray();
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
