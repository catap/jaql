package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JDouble;
import com.ibm.jaql.json.type.JValue;

public class JDoubleSerializer extends BasicSerializer<JDouble>
{

  @Override
  public JDouble newInstance()
  {
    return new JDouble();
  }


  @Override
  public JDouble read(DataInput in, JValue target) throws IOException
  {
    double value = in.readDouble();
    if (target == null || !(target instanceof JDouble)) {
      return new JDouble(value);
    } else {
      JDouble t = (JDouble)target;
      t.value = value;
      return t;
    }
  }


  @Override
  public void write(DataOutput out, JDouble value) throws IOException
  {
    out.writeDouble(value.value);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
