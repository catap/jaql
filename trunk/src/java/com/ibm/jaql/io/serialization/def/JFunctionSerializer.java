package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.lang.core.JFunction;

public class JFunctionSerializer extends BasicSerializer<JFunction>
{
  @Override
  public JFunction newInstance()
  {
    return new JFunction();
  }

  @Override
  public JFunction read(DataInput in, JValue target) throws IOException
  {
    JFunction t;
    if (target==null || !(target instanceof JFunction)) {
      t = new JFunction();
    } else {
      t = (JFunction)target;
    }
      
    String fnText = in.readUTF();
    try {
      t.set(fnText);
    } catch (Exception e) {
      throw new UndeclaredThrowableException(e);
    }
    return t;
  }

  @Override
  public void write(DataOutput out, JFunction value) throws IOException
  {
    out.writeUTF(value.toJSON());
  }

}
