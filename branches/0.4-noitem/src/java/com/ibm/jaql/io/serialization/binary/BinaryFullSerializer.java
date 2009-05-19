package com.ibm.jaql.io.serialization.binary;

import java.io.DataInput;
import java.io.DataOutput;

import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;

/** Full serializer for binary data.
 * 
 * @param <T> type of value to work on
 */
public abstract class BinaryFullSerializer extends FullSerializer<DataInput, DataOutput>
{
  // -- default serializer  ----------------------------------------------------------------------
  
  private static final BinaryFullSerializer DEFAULT_SERIALIZER = DefaultBinaryFullSerializer.getInstance();
  
  public static BinaryFullSerializer getDefault()
  {
    return DEFAULT_SERIALIZER;
  }
}
