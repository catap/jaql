package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JSpan;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.util.BaseUtil;

public class JSpanSerializer extends BasicSerializer<JSpan>
{

  @Override
  public JSpan newInstance()
  {
    return new JSpan();
  }

  @Override
  public JSpan read(DataInput in, JValue target) throws IOException
  {
    JSpan t;
    if (target == null || !(target instanceof JSpan)) {
      t = new JSpan();
    } else {
      t = (JSpan)target;
    }
    t.begin = BaseUtil.readVULong(in);
    t.end = t.begin + BaseUtil.readVULong(in);
    
    return t;
  }


  @Override
  public void write(DataOutput out, JSpan value) throws IOException
  {
    BaseUtil.writeVULong(out, value.begin);
    BaseUtil.writeVULong(out, value.end - value.begin);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
