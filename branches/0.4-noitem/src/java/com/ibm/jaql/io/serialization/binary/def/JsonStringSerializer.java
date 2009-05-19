package com.ibm.jaql.io.serialization.binary.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

public class JsonStringSerializer extends BinaryBasicSerializer<JsonString>
{
  @Override
  public JsonString newInstance()
  {
    return new JsonString();
  }

  @Override
  public JsonString read(DataInput in, JsonValue target) throws IOException
  {
    int length = BaseUtil.readVUInt(in);
    
    JsonString t;
    byte[] bytes;
    if (target==null || !(target instanceof JsonString)) {
      bytes = new byte[length];
      t = new JsonString(bytes);
    } else {
      t = (JsonString)target;
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
  public void write(DataOutput out, JsonString value) throws IOException
  {
    int length = value.getLength();
    BaseUtil.writeVUInt(out, length);
    out.write(value.getInternalBytes(), 0, length);
    
  }

  //TODO: efficient implementation of compare, skip, and copy
}
