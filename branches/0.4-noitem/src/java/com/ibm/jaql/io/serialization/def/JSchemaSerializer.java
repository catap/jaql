package com.ibm.jaql.io.serialization.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JSchema;
import com.ibm.jaql.json.type.JValue;

public class JSchemaSerializer extends BasicSerializer<JSchema> 
{

  @Override
  public JSchema newInstance()
  {
    return new JSchema();
  }

  @Override
  public JSchema read(DataInput in, JValue target) throws IOException
  {
    Schema schema = Schema.read(in);
    if (target == null || !(target instanceof JSchema)) {
      return new JSchema(schema);
    } else {
      JSchema t = (JSchema)target;
      t.setSchema(schema);
      return t;
    }
  }

  @Override
  public void write(DataOutput out, JSchema value) throws IOException
  {
    value.getSchema().write(out);
  }
}
