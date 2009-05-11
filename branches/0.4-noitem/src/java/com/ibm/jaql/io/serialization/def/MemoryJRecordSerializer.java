package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.util.BaseUtil;

public class MemoryJRecordSerializer extends BasicSerializer<MemoryJRecord>
{
  BasicSerializer<JString> nameSerializer;
  FullSerializer valueSerializer;

  public MemoryJRecordSerializer(BasicSerializer<JString> nameSerializer, FullSerializer valueSerializer)
  {
    this.nameSerializer = nameSerializer ;
    this.valueSerializer = valueSerializer;
  }

  
  @Override
  public MemoryJRecord newInstance()
  {
    return new MemoryJRecord();
  }


  @Override
  public MemoryJRecord read(DataInput in, JValue target) throws IOException
  {
    int arity = BaseUtil.readVUInt(in);
    MemoryJRecord t;
    if (target==null || !(target instanceof MemoryJRecord)) {
      t = new MemoryJRecord(arity);
    } else {
      t = (MemoryJRecord)target;
      t.ensureCapacity(arity);
    }

    JString[] names = t.getInternalNamesArray();
    Item[] values = t.getInternalValueArray();
    for (int i = 0; i < arity; i++)
    {
      names[i] = (JString)nameSerializer.read(in, names[i]);
      values[i].set(valueSerializer.read(in, values[i].get()));
    }
    
    t.set(names, values, arity);    
    return t;
  }


  @Override
  public void write(DataOutput out, MemoryJRecord value) throws IOException
  {
    int arity = value.arity();
    BaseUtil.writeVUInt(out, arity);
    for (int i = 0; i < arity; i++)
    {
      nameSerializer.write(out, value.getName(i));
      valueSerializer.write(out, value.getValue(i).get());
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

