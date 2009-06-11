package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JDate;
import com.ibm.jaql.json.type.JValue;

public class JDateSerializer extends BasicSerializer<JDate>
{

  @Override
  public JDate newInstance()
  {
    return new JDate();
  }


  @Override
  public JDate read(DataInput in, JValue target) throws IOException
  {
    long millis = in.readLong();
    if (target == null || !(target instanceof JDate)) {
      return new JDate(millis);
    } else {
      JDate t = (JDate)target;
      t.millis = millis;
      return t;
    }
  }


  @Override
  public void write(DataOutput out, JDate value) throws IOException
  {
    out.writeLong(value.millis);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
