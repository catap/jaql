/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql.io.serialization;

import java.io.IOException;

import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;

/** Superclass for serializers of <code>JValue</code>s of unknown type. 
 * <code>FullSerializer</code>s embed type information into their output. They make use of
 * {@link BasicSerializer}s to serialize the actual values. A particular 
 * <code>FullSerializer</code> can only read values that have been written by itself.
 * 
 * @param <In> type of input
 * @param <Out> type of output
 */
public abstract class FullSerializer<In, Out> implements Serializer<In, Out, JsonValue>
{
  // -- abstract methods -------------------------------------------------------------------------
  
  @Override
  public abstract JsonValue read(In in, JsonValue target) throws IOException;
  
  @Override
  public abstract void write(Out out, JsonValue value)  throws IOException;
  
  
  // -- default implementations ------------------------------------------------------------------
  
  @Override
  public void skip(In in) throws IOException {
    read(in, null);
  }
  
  @Override
  public int compare(In in1, In in2) throws IOException {
    JsonValue v1 = read(in1, null);
    JsonValue v2 = read(in2, null);
    return JsonUtil.compare(v1, v2);
  }
  
  @Override
  public void copy(In in, Out out) throws IOException {
    JsonValue v = read(in, null);
    write(out, v);
  }
  

  // -- utility methods --------------------------------------------------------------------------

  /** Compares two encoded item arrays of known length without decoding them. This method (1) never 
   * reads more bytes than used by the encoded arrays, (2) does not read more bytes than necessary 
   * to make the decision, and (3) guarantees to read the entire encoded arrays in case of 
   * equality.  
   *  
   * @param input1 input pointing to the first item of the first array
   * @param count1 length of first array
   * @param input2 input pointing to the first item of the second array
   * @param count2 length of second array
   * @param serializer serializer used to store elements of the array 
   * @return
   * @throws IOException
   */
  public static <In> int compareArrays(In input1, int count1, In input2, int count2, 
      FullSerializer<In,?> serializer) 
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
   * @param input1 input pointing to the first item of the first array
   * @param count1 length of first array
   * @param input2 input pointing to the first item of the second array
   * @param count2 length of second array
   * @param serializer serializer used to store elements of the array 
   * @return
   * @throws IOException
   */
  public static <In> int compareArrays(In input1, long count1, In input2, long count2, 
      FullSerializer<In,?> serializer) 
  throws IOException {
    long m = Math.min(count1, count2);
    for (long i=0; i<m; i++) {
      int cmp = serializer.compare(input1, input2);
      if (cmp != 0) return cmp;
    }
    return count1<count2 ? -1 : (count1==count2 ? 0 : 1);  
  }
}
