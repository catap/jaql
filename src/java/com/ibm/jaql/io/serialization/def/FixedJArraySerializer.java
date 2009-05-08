package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.util.BaseUtil;

public class FixedJArraySerializer extends BasicSerializer<FixedJArray>
{
  FullSerializer fullSerializer;
  
  public FixedJArraySerializer(FullSerializer fullSerializer) {
    this.fullSerializer = fullSerializer;
  }
  
  @Override
  public FixedJArray newInstance()
  {
    return new FixedJArray();
  }

  @Override
  public FixedJArray read(DataInput in, JValue target) throws IOException
  {
    int n = BaseUtil.readVUInt(in);
    FixedJArray t;
    if (target==null || !(target instanceof FixedJArray)) {
      t = new FixedJArray(n);
    } else {
      t = (FixedJArray)target;
      t.resize(n);
    }
    
    for (int i = 0; i < n; i++)
    {
      Item item = t.get(i);
      if (item == null)
      {
        item = new Item();
        t.set(i, item);
      }
      JValue v = fullSerializer.read(in, item.get());
      item.set(v);
    }
    
    return t;
  }

  @Override
  public void write(DataOutput out, FixedJArray value) throws IOException
  {
    // update AscDescItemComparator when changing this

    int n = value.size();
    BaseUtil.writeVUInt(out, n);
    for (int i = 0; i < n; i++)
    {
      fullSerializer.write(out, value.get(i).get());
    }
  }
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    int n1 = BaseUtil.readVUInt(in1);
    int n2 = BaseUtil.readVUInt(in2);
    return FullSerializer.compareArrays(in1, n1, in2, n2, fullSerializer);
  }
  
  //TODO: efficient implementation of skip, and copy

}
