package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.util.BaseUtil;

public class BufferedJsonRecordSerializer extends BasicSerializer<BufferedJsonRecord>
{
  BasicSerializer<JsonString> nameSerializer;
  FullSerializer valueSerializer;

  public BufferedJsonRecordSerializer(BasicSerializer<JsonString> nameSerializer, FullSerializer valueSerializer)
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
    
    t.set(names, values, arity);    
    return t;
  }


  @Override
  public void write(DataOutput out, BufferedJsonRecord value) throws IOException
  {
    int arity = value.arity();
    BaseUtil.writeVUInt(out, arity);
    for (int i = 0; i < arity; i++)
    {
      nameSerializer.write(out, value.getName(i));
      valueSerializer.write(out, value.getValue(i));
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

