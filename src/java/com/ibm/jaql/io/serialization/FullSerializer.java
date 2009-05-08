package com.ibm.jaql.io.serialization;

import java.io.DataInput;
import java.io.IOException;

import com.ibm.jaql.io.serialization.def.DefaultFullSerializer;
import com.ibm.jaql.json.type.JValue;

/** Superclass for serializers of <code>JValue</code>s of unknown type. 
 * <code>FullSerializer</code>s embed type information into their output. They make use of
 * {@link BasicSerializer}s to serialize the actual values. A particular 
 * <code>FullSerializer</code> can only read values that have been written by itself. 
 */
public abstract class FullSerializer extends AbstractSerializer<JValue>
{
  // -- default serializer  ----------------------------------------------------------------------
  
  private static FullSerializer defaultSerializer = DefaultFullSerializer.getDefaultInstance();
  
  public static void setDefault(FullSerializer serializer) {
    defaultSerializer = serializer;
  }
  
  public static FullSerializer getDefault()
  {
    return defaultSerializer;
  }

  
  // -- utility methods --------------------------------------------------------------------------

  /** Compares two encoded item arrays of known length without decoding them. This method (1) never 
   * reads more bytes than used by the encoded arrays, (2) does not read more bytes than necessary 
   * to make the decision, and (3) guarantees to read the entire encoded arrays in case of 
   * equality.  
   *  
   * @param input1 input stream pointing to the first item of the first array
   * @param count1 length of first array
   * @param input2 input stream pointing to the first item of the second array
   * @param count2 length of second array
   * @param serializer serializer used to store elements of the array 
   * @return
   * @throws IOException
   */
  public static int compareArrays(DataInput input1, int count1, DataInput input2, int count2, 
      FullSerializer serializer) 
  throws IOException {
    int m = Math.min(count1, count2);
    for (int i=0; i<m; i++) {
      int cmp = serializer.compare(input1, input2);
      if (cmp != 0) return cmp;
    }
    return count1-count2;  
  }
  
  /** Compares two encoded item arrays of known length without decoding them. This method (1) never 
   * reads more bytes than used by the encoded arrays, (2) does not read more bytes than necessary 
   * to make the decision, and (3) guarantees to read the entire encoded arrays in case of 
   * equality.  
   *  
   * @param input1 input stream pointing to the first item of the first array
   * @param count1 length of first array
   * @param input2 input stream pointing to the first item of the second array
   * @param count2 length of second array
   * @param serializer serializer used to store elements of the array 
   * @return
   * @throws IOException
   */
  public static int compareArrays(DataInput input1, long count1, DataInput input2, long count2, 
      FullSerializer serializer) 
  throws IOException {
    long m = Math.min(count1, count2);
    for (long i=0; i<m; i++) {
      int cmp = serializer.compare(input1, input2);
      if (cmp != 0) return cmp;
    }
    return count1<count2 ? -1 : (count1==count2 ? 0 : 1);  
  }
}
