package com.ibm.jaql.io.serialization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.json.type.JValue;

/** Provides default implementation for some methods of {@link Serializer}. The default 
 * implementations are based on deserialization. They should be overwritten whereever
 * efficiency is a concern. */
public abstract class AbstractSerializer<T extends JValue> implements Serializer<T>
{
  public void skip(DataInput in) throws IOException {
    read(in, null);
  }
  
  public int compare(DataInput in1, DataInput in2) throws IOException {
    T v1 = read(in1, null);
    T v2 = read(in2, null);
    return v1.compareTo(v2);
  }
  
  public void copy(DataInput in, DataOutput out) throws IOException {
    T v = read(in, null);
    write(out, v);
  }
}
