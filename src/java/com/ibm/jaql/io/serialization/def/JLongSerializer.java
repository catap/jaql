package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.util.BaseUtil;

public class JLongSerializer extends BasicSerializer<JLong>
{

  @Override
  public JLong newInstance()
  {
    return new JLong();
  }

  @Override
  public JLong read(DataInput in, JValue target) throws IOException
  {
    long value = BaseUtil.readVSLong(in); 
    if (target == null || !(target instanceof JLong)) {
      return new JLong(value);
    } else {
      JLong t = (JLong)target;
      t.setValue(value);
      return t;
    }
  }


  @Override
  public void write(DataOutput out, JLong value) throws IOException
  {
    BaseUtil.writeVSLong(out, value.value);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
