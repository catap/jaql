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

import com.ibm.jaql.json.type.JsonValue;

/** Main interface for operations on serialized data. 
 *
 * @param <In> type of input
 * @param <Out> type of output
 * @param <T> type of value to work on
 * 
 */
public interface Serializer<In, Out, T extends JsonValue>
{
  /** Reads a value from <code>in</code>.The specified target value is reused, if possible. 
   * 
   * @param in input
   * @param target a value to be reused (optional, can be <code>null</code>)
   * @return the read value. May or may not be equal to <code>target</code>.
   * @throws IOException
   */
  public T read(In in, JsonValue target) throws IOException;
  
  /** Writes a value to <code>out</code>.
   * 
   * @param out output
   * @param value a value
   * @throws IOException
   */
  public void write(Out out, T value)  throws IOException;
  
  /** Skips the next value in the specified data input.
   * 
   * @param in input 
   * @throws IOException
   */
  public void skip(In in) throws IOException;
  
  /** Compares the encoded value from <code>in1</code> with the encoded value from 
   * <code>in2</code>. Comparison is usually performed without decoding. This method 
   * (1) never reads more bytes than used by the encoded values, (2) tries to not read more 
   * bytes than necessary to make the decision, and (3) guarantees to read the entire encoded 
   * values in case of equality.   
   * 
   * @param in1 an input pointing to a value
   * @param in2 another input pointing to another value
   * @return
   * @throws IOException
   */
  public int compare(In in1, In in2) throws IOException;
  
  /** Copies the next value from <code>in</code> to <code>out</code>. 
   * 
   * @param in input from which value is read
   * @param out output to which value is copied
   * @throws IOException
   */
  public void copy(In in, Out out) throws IOException;
}
