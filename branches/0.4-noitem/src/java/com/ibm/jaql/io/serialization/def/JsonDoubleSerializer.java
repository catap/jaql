package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonValue;

public class JsonDoubleSerializer extends BasicSerializer<JsonDouble>
{

  @Override
  public JsonDouble newInstance()
  {
    return new JsonDouble();
  }


  @Override
  public JsonDouble read(DataInput in, JsonValue target) throws IOException
  {
    double value = in.readDouble();
    if (target == null || !(target instanceof JsonDouble)) {
      return new JsonDouble(value);
    } else {
      JsonDouble t = (JsonDouble)target;
      t.value = value;
      return t;
    }
  }


  @Override
  public void write(DataOutput out, JsonDouble value) throws IOException
  {
    out.writeDouble(value.value);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
