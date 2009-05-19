package com.ibm.jaql.io.serialization.text;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.io.serialization.text.def.DefaultTextFullSerializer;
import com.ibm.jaql.json.type.JsonValue;

/** Full serializer for character data.
 * 
 * @param <T> type of value to work on
 */
public abstract class TextFullSerializer extends FullSerializer<InputStream, PrintStream>
{
  
  @Override
  public void write(PrintStream out, JsonValue value) throws IOException
  {
    write(out, value, 0);
  }
  
  public abstract void write(PrintStream out, JsonValue value, int indent) throws IOException;
  
  // -- default serializer  ----------------------------------------------------------------------
  
  private static TextFullSerializer DEFAULT_SERIALIZER = DefaultTextFullSerializer.getInstance();
  
  public static void setDefault(TextFullSerializer serializer) {
    DEFAULT_SERIALIZER = serializer;
  }
  
  public static TextFullSerializer getDefault()
  {
    return DEFAULT_SERIALIZER;
  }
}
