package com.ibm.jaql.io.serialization.text;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.BasicSerializer;
import com.ibm.jaql.json.type.JsonValue;

/** Basic serializer for character data.
 * 
 * @param <T> type of value to work on
 */
public abstract class TextBasicSerializer<T extends JsonValue> 
extends BasicSerializer<InputStream, PrintStream, T>
{
  public T newInstance()
  {
    throw new UnsupportedOperationException();
  }
  
  /** Unsupported operation. */
  @Override
  public T read(InputStream in, JsonValue target) throws IOException
  {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void write(PrintStream out, T value) throws IOException
  {
    write(out, value, 0);
  }
  
  public abstract void write(PrintStream out, T value, int indent) throws IOException;
  
  public static final void indent(PrintStream out, int indent)
  {
    for (int i=0; i<indent; i++)
    {
      out.print(" ");
    }
  }
}
