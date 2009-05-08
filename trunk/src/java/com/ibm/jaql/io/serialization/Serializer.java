package com.ibm.jaql.io.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.json.type.JValue;

/** Main interface for operations on serialized data. */
public interface Serializer<T extends JValue>
{
  /** Reads a value from <code>in</code>.The specified target value is reused, if possible. 
   * 
   * @param in data input
   * @param target a value to be reused (optional, can be <code>null</code>)
   * @return the read value. May or may not be equal to <code>target</code>.
   * @throws IOException
   */
  public T read(DataInput in, JValue target) throws IOException;
  
  /** Writes a value to <code>out</code>.
   * 
   * @param out data output
   * @param value a value
   * @throws IOException
   */
  public void write(DataOutput out, T value)  throws IOException;
  
  /** Skips the next value in the specified data input.
   * 
   * @param in data input 
   * @throws IOException
   */
  public void skip(DataInput in) throws IOException;
  
  /** Compares the encoded value from <code>in1</code> with the encoded value from 
   * <code>in2</code>. Comparison is usually performed without decoding. This method 
   * (1) never reads more bytes than used by the encoded values, (2) tries to not read more 
   * bytes than necessary to make the decision, and (3) guarantees to read the entire encoded 
   * values in case of equality.   
   * 
   * @param in1 an input stream pointing to a value
   * @param in2 another input stream pointing to another value
   * @return
   * @throws IOException
   */
  public int compare(DataInput in1, DataInput in2) throws IOException;
  
  /** Copies the next value from <code>in</code> to <code>out</code>. 
   * 
   * @param in data input from which value is read
   * @param out data output to which value is copied
   * @throws IOException
   */
  public void copy(DataInput in, DataOutput out) throws IOException;
}
