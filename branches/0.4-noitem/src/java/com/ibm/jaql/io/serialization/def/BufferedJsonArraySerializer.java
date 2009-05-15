package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

public class BufferedJsonArraySerializer extends BasicSerializer<BufferedJsonArray>
{
  FullSerializer fullSerializer;
  
  public BufferedJsonArraySerializer(FullSerializer fullSerializer) {
    this.fullSerializer = fullSerializer;
  }
  
  @Override
  public BufferedJsonArray newInstance()
  {
    return new BufferedJsonArray();
  }

  @Override
  public BufferedJsonArray read(DataInput in, JsonValue target) throws IOException
  {
    int n = BaseUtil.readVUInt(in);
    BufferedJsonArray t;
    if (target==null || !(target instanceof BufferedJsonArray)) {
      t = new BufferedJsonArray(n);
    } else {
      t = (BufferedJsonArray)target;
      t.resize(n);
    }
    
    for (int i = 0; i < n; i++)
    {
      JsonValue value = t.get(i);
      value = fullSerializer.read(in, value);
      t.set(i, value);      
    }
    
    return t;
  }

  @Override
  public void write(DataOutput out, BufferedJsonArray value) throws IOException
  {
    // update AscDescItemComparator when changing this

    int n = value.size();
    BaseUtil.writeVUInt(out, n);
    for (int i = 0; i < n; i++)
    {
      fullSerializer.write(out, value.get(i));
    }
  }
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    int n1 = BaseUtil.readVUInt(in1);
    int n2 = BaseUtil.readVUInt(in2);
    return FullSerializer.compareArrays(in1, n1, in2, n2, fullSerializer);
  }
  
  //TODO: efficient implementation of skip, and copy

}
