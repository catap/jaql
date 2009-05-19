package com.ibm.jaql.io.serialization.binary.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class JsonRegexSerializer extends BinaryBasicSerializer<JsonRegex>
{
  BinaryBasicSerializer<JsonString> stringSerializer;
  
  public JsonRegexSerializer(BinaryBasicSerializer<JsonString> stringSerializer) {
    this.stringSerializer = stringSerializer;
  }
  
  @Override
  public JsonRegex newInstance()
  {
    return new JsonRegex();
  }

  @Override
  public JsonRegex read(DataInput in, JsonValue target) throws IOException
  {
    if (target==null || !(target instanceof JsonRegex)) {
      JsonString regex = stringSerializer.read(in, null);
      byte flags = in.readByte();
      return new JsonRegex(regex, flags);
    } else {
      JsonRegex t = (JsonRegex)target;
      JsonString regex = stringSerializer.read(in, t.getInternalRegex());
      byte flags = in.readByte();
      t.set(regex, flags);
      return t;
    }
  }

  @Override
  public void write(DataOutput out, JsonRegex value) throws IOException
  {
    stringSerializer.write(out, value.getInternalRegex());
    out.writeByte(value.getFlags());
  }

  //TODO: efficient implementation of compare, skip, and copy
}
