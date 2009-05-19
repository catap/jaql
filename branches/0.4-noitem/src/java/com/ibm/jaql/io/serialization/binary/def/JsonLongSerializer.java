package com.ibm.jaql.io.serialization.binary.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

public class JsonLongSerializer extends BinaryBasicSerializer<JsonLong>
{

  @Override
  public JsonLong newInstance()
  {
    return new JsonLong();
  }

  @Override
  public JsonLong read(DataInput in, JsonValue target) throws IOException
  {
    long value = BaseUtil.readVSLong(in); 
    if (target == null || !(target instanceof JsonLong)) {
      return new JsonLong(value);
    } else {
      JsonLong t = (JsonLong)target;
      t.setValue(value);
      return t;
    }
  }


  @Override
  public void write(DataOutput out, JsonLong value) throws IOException
  {
    BaseUtil.writeVSLong(out, value.value);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
