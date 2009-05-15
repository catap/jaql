package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonValue;

public class JsonDateSerializer extends BasicSerializer<JsonDate>
{

  @Override
  public JsonDate newInstance()
  {
    return new JsonDate();
  }


  @Override
  public JsonDate read(DataInput in, JsonValue target) throws IOException
  {
    long millis = in.readLong();
    if (target == null || !(target instanceof JsonDate)) {
      return new JsonDate(millis);
    } else {
      JsonDate t = (JsonDate)target;
      t.millis = millis;
      return t;
    }
  }


  @Override
  public void write(DataOutput out, JsonDate value) throws IOException
  {
    out.writeLong(value.millis);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
