package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JsonSpan;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.BaseUtil;

public class JsonSpanSerializer extends BasicSerializer<JsonSpan>
{

  @Override
  public JsonSpan newInstance()
  {
    return new JsonSpan();
  }

  @Override
  public JsonSpan read(DataInput in, JsonValue target) throws IOException
  {
    JsonSpan t;
    if (target == null || !(target instanceof JsonSpan)) {
      t = new JsonSpan();
    } else {
      t = (JsonSpan)target;
    }
    t.begin = BaseUtil.readVULong(in);
    t.end = t.begin + BaseUtil.readVULong(in);
    
    return t;
  }


  @Override
  public void write(DataOutput out, JsonSpan value) throws IOException
  {
    BaseUtil.writeVULong(out, value.begin);
    BaseUtil.writeVULong(out, value.end - value.begin);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
