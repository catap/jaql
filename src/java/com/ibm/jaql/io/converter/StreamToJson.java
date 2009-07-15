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
package com.ibm.jaql.io.converter;

import java.io.IOException;
import java.io.InputStream;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;

/** Interface for reading {@link JsonValue}s from an {@link InputStream}. <code>StreanToJson</code>
 * converters can read the entire stream to produce a single JSON value or, alternatively, produce 
 * an array by providing its elements one at a time. */
public interface StreamToJson<T extends JsonValue>
{
  /** Sets the input stream to read from. */
  void setInput(InputStream in);
  
  /**
   * If the converter is for array access, then it assumes the stream encodes a JSON array.
   * In this case, one JSON value at-a-time is read. Otherwise, the entire stream is read
   * to produce a single JSON value.
   *  
   * @param a
   */
  void setArrayAccessor(boolean a);
  
  /**
   * Is the converter an array accessor?
   * 
   * @return
   */
  boolean isArrayAccessor();

  // TODO: synchronize with ToJson API (i.e., add a createtarget method)
  /** Read from the stream and return the result. Implementations may 
   * reuse the <code>target</code> argument to improve efficiency. Callers to this method will 
   * provide either <code>null</code> or the result of a previous call to this method.
   * 
   * @param target
   * @return
   * @throws IOException
   */
  T read(T target) throws IOException;
  
  /** Describes the schema of the value(s) produced by {@link #read(JsonValue)}}. If this class
   * is configured for array access, this method should return the schema of each individual
   * element of the array. Implementations should provide as much information as possible to 
   * facilitate query optimization. If no information about the schema is known, return 
   * {@link SchemaFactory#anyOrNullSchema()}.
   * 
   * @return a schema that all values produced by this converter adhere to
   */
  Schema getSchema();
}
