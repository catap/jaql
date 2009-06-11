package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JBinary;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.util.BaseUtil;

/** Default serializer for {@link JBinary}. */
public class JBinarySerializer extends BasicSerializer<JBinary>
{
  @Override
  public JBinary newInstance()
  {
    return new JBinary();
  }
  
  @Override
  public JBinary read(DataInput in, JValue target) throws IOException
  {
    int length = BaseUtil.readVUInt(in);
    byte[] bytes;
    JBinary t;
    if (target == null || !(target instanceof JBinary)) {
      bytes = new byte[length];
      t = new JBinary(bytes);
    } else {
      t = (JBinary)target;
      t.ensureCapacity(length);
      bytes = t.getInternalBytes();
    }
    in.readFully(bytes, 0, length);
 
    return t;
  }

  @Override
  public void write(DataOutput out, JBinary value) throws IOException
  {
    int length = value.getLength();
    BaseUtil.writeVUInt(out, length);
    out.write(value.getInternalBytes(), 0, length);
  }

  @Override
  public int compare(DataInput in1, DataInput in2) throws IOException
  {
    int l1 = BaseUtil.readVUInt(in1);
    int l2 = BaseUtil.readVUInt(in2);

    int m = Math.min(l1, l2);
    for (int i=0; i<m; i++) {
      int b1 = in1.readByte();
      int b2 = in2.readByte();
      if (b1 != b2) {
        return (b1 & 0xff) - (b2 & 0xff);
      }
    }
        
    return l1 - l2;
  }

  @Override
  public void skip(DataInput in) throws IOException
  {
    int length = BaseUtil.readVUInt(in);
    in.skipBytes(length);
  }
  
  // TODO: efficient implementation of copy

}
