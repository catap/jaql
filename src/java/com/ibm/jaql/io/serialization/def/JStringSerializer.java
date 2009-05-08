package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.util.BaseUtil;

public class JStringSerializer extends BasicSerializer<JString>
{
  @Override
  public JString newInstance()
  {
    return new JString();
  }

  @Override
  public JString read(DataInput in, JValue target) throws IOException
  {
    int length = BaseUtil.readVUInt(in);
    
    JString t;
    byte[] bytes;
    if (target==null || !(target instanceof JString)) {
      bytes = new byte[length];
      t = new JString(bytes);
    } else {
      t = (JString)target;
      bytes = t.getInternalBytes();
      if (bytes.length < length) {
        bytes = new byte[length];
      }
    }
    in.readFully(bytes, 0, length);
    t.set(bytes, length);
    return t;
  }

  @Override
  public void write(DataOutput out, JString value) throws IOException
  {
    int length = value.getLength();
    BaseUtil.writeVUInt(out, length);
    out.write(value.getInternalBytes(), 0, length);
    
  }

  //TODO: efficient implementation of compare, skip, and copy
}
