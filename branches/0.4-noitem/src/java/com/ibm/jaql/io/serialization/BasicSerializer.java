package com.ibm.jaql.io.serialization;

import java.io.IOException;

import com.ibm.jaql.json.type.JsonValue;

/** Superclass for serializers of <code>JValue</code>s of known type and encoding. Each 
 * <code>BasicSerializer</code> is associated with a particular implementing class <code>T</code> 
 * of {@link JsonValue}. It can only read and write values of type <code>T</code>. Moreover,
 * values that are read have to be written by the same <code>BasicSerializer</code>. 
 * 
 * See {@link FullSerializer} serializers that extract type information from the input.
 *
 * @param <In> type of input
 * @param <Out> type of output
 * @param <T> type of value to work on
 */
public abstract class BasicSerializer<In, Out, T extends JsonValue> 
implements Serializer<In, Out, T>
{

  // -- abstract methods -------------------------------------------------------------------------
  
  /** Creates a new instance of the value corresponding to this serializer. */
  public abstract T newInstance();

  @Override
  public abstract T read(In in, JsonValue target) throws IOException;
  
  @Override
  public abstract void write(Out out, T value)  throws IOException;
  
  
  // -- default implementations ------------------------------------------------------------------
  
  @Override
  public void skip(In in) throws IOException {
    read(in, null);
  }
  
  @Override
  public int compare(In in1, In in2) throws IOException {
    T v1 = read(in1, null);
    T v2 = read(in2, null);
    return v1.compareTo(v2);
  }
  
  @Override
  public void copy(In in, Out out) throws IOException {
    T v = read(in, null);
    write(out, v);
  }
}
