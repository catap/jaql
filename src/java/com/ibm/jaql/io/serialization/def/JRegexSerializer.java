package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JRegex;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;

public class JRegexSerializer extends BasicSerializer<JRegex>
{
  BasicSerializer<JString> stringSerializer;
  
  public JRegexSerializer(BasicSerializer<JString> stringSerializer) {
    this.stringSerializer = stringSerializer;
  }
  
  @Override
  public JRegex newInstance()
  {
    return new JRegex();
  }

  @Override
  public JRegex read(DataInput in, JValue target) throws IOException
  {
    if (target==null || !(target instanceof JRegex)) {
      JString regex = stringSerializer.read(in, null);
      byte flags = in.readByte();
      return new JRegex(regex, flags);
    } else {
      JRegex t = (JRegex)target;
      JString regex = stringSerializer.read(in, t.getInternalRegex());
      byte flags = in.readByte();
      t.set(regex, flags);
      return t;
    }
  }

  @Override
  public void write(DataOutput out, JRegex value) throws IOException
  {
    stringSerializer.write(out, value.getInternalRegex());
    out.writeByte(value.getFlags());
  }

  //TODO: efficient implementation of compare, skip, and copy
}
