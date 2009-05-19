package com.ibm.jaql.io.serialization.binary.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.JaqlFunction;

public class JaqlFunctionSerializer extends BinaryBasicSerializer<JaqlFunction>
{
  @Override
  public JaqlFunction newInstance()
  {
    return new JaqlFunction();
  }

  @Override
  public JaqlFunction read(DataInput in, JsonValue target) throws IOException
  {
    JaqlFunction t;
    if (target==null || !(target instanceof JaqlFunction)) {
      t = new JaqlFunction();
    } else {
      t = (JaqlFunction)target;
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
  public void write(DataOutput out, JaqlFunction value) throws IOException
  {
    out.writeUTF(value.getText());
  }

}
