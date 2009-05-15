package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonValue;

public class JsonBoolSerializer extends BasicSerializer<JsonBool>
{

  @Override
  public JsonBool newInstance()
  {
    return new JsonBool();
  }

  @Override
  public JsonBool read(DataInput in, JsonValue target) throws IOException
  {
    boolean value = in.readByte() != 0; 
    if (target == null || !(target instanceof JsonBool)) {
      return new JsonBool(value);
    } else {
      JsonBool t = (JsonBool)target;
      t.setValue(value);
      return t;
    }
  }

  @Override
  public void write(DataOutput out, JsonBool value) throws IOException
  {
    out.writeByte(value.getValue() ? 1 : 0);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
