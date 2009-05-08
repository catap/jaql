package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JDecimal;
import com.ibm.jaql.json.type.JValue;

public class JDecimalSerializer extends BasicSerializer<JDecimal>
{

  @Override
  public JDecimal newInstance()
  {
    return new JDecimal();
  }

  @Override
  public JDecimal read(DataInput in, JValue target) throws IOException
  {
    // TODO: need to read and write binary or at least avoid alloc string
    // TODO: need to cache bigdecimal
    String str = in.readUTF();
    BigDecimal value = new BigDecimal(str);
    if (target == null || !(target instanceof JDecimal)) {
      return new JDecimal(value);
    } else {
      JDecimal t = (JDecimal)target;
      t.setValue(value);
      return t;
    }
  }


  @Override
  public void write(DataOutput out, JDecimal value) throws IOException
  {
    String str = value.value.toString();
    out.writeUTF(str);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
