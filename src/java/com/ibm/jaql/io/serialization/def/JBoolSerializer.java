package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.json.type.JValue;

public class JBoolSerializer extends BasicSerializer<JBool>
{

  @Override
  public JBool newInstance()
  {
    return new JBool();
  }

  @Override
  public JBool read(DataInput in, JValue target) throws IOException
  {
    boolean value = in.readByte() != 0; 
    if (target == null || !(target instanceof JBool)) {
      return new JBool(value);
    } else {
      JBool t = (JBool)target;
      t.setValue(value);
      return t;
    }
  }

  @Override
  public void write(DataOutput out, JBool value) throws IOException
  {
    out.writeByte(value.getValue() ? 1 : 0);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
