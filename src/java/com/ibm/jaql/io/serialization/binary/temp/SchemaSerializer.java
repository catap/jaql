package com.ibm.jaql.io.serialization.binary.temp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.json.schema.SchematypeSchema;
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;


public class SchemaSerializer extends BinaryBasicSerializer<JsonSchema>
{
  private SchematypeSchema schema;
  private BinaryBasicSerializer<JsonSchema> serializer;
      
  
  // -- construction ------------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  public SchemaSerializer(SchematypeSchema schema)
  {
    this.schema = schema; 
    this.serializer = (BinaryBasicSerializer<JsonSchema>)DefaultBinaryFullSerializer.getInstance()
        .getSerializer(JsonEncoding.SCHEMA);
  }

  // -- serialization -----------------------------------------------------------------------------

  @Override
  public JsonSchema read(DataInput in, JsonValue target) throws IOException
  {
    return serializer.read(in, target);
  }

  @Override
  public void write(DataOutput out, JsonSchema value) throws IOException
  {
    // check match
    if (!schema.matches(value))
    {
      throw new IllegalArgumentException("value not matched by this serializer");
    }

    // write
    serializer.write(out, value);
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
