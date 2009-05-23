package com.ibm.jaql.io.serialization.binary.def;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;

import antlr.RecognitionException;
import antlr.TokenStreamException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;

public class JsonSchemaSerializer extends BinaryBasicSerializer<JsonSchema> 
{
  private com.ibm.jaql.io.serialization.text.def.JsonSchemaSerializer textSerializer
    = new com.ibm.jaql.io.serialization.text.def.JsonSchemaSerializer(); 
  
  @Override
  public JsonSchema newInstance()
  {
    return new JsonSchema();
  }

  @Override
  public JsonSchema read(DataInput in, JsonValue target) throws IOException
  {
    String s = in.readUTF();
    JaqlLexer lexer = new JaqlLexer(new StringReader(s));
    JaqlParser parser = new JaqlParser(lexer);
    Schema schema;
    try
    {
      schema = parser.schema();
    } catch (RecognitionException e)
    {
      throw new IOException(e);
    } catch (TokenStreamException e)
    {
      throw new IOException(e);
    }

    if (target == null || !(target instanceof JsonSchema)) {
      return new JsonSchema(schema);
    } else {
      JsonSchema t = (JsonSchema)target;
      t.setSchema(schema);
      return t;
    }
  }

  @Override
  public void write(DataOutput out, JsonSchema value) throws IOException
  {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream pout = new PrintStream(bout);
    textSerializer.write(pout, value);
    pout.flush();
    String s = bout.toString();
    out.writeUTF(s);
  }
}
