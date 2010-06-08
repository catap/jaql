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
import java.io.OutputStream;

import com.ibm.jaql.io.Initializable;
import com.ibm.jaql.json.type.JsonValue;

/**
 * Interface for writing {@link JsonValue}s to an {@link OutputStream}. It has
 * two modes. Array access mode is for writing items in a JSON array. Non-array
 * access mode is for writing a single JSON value.
 */
public interface JsonToStream<T extends JsonValue> extends Initializable {
  /**
   * Sets output stream.
   * 
   * @param out Output stream
   */
  void setOutputStream(OutputStream out);

  /**
   * If the converter is not for array access, then it assumes the JSON value is
   * not part of a JSON array. In this case, only one value is expected to be
   * written out.
   * 
   * @param a
   */
  void setArrayAccessor(boolean a);

  /**
   * Tests whether the converter is an array accessor.
   * 
   * @return <code>true</code> if the converter is an array accessor:
   *         <code>false</code> otherwise.
   */
  boolean isArrayAccessor();

  /**
   * Writes the JSON value to the output stream.
   * 
   * @param i JSON value
   * @throws IOException
   */
  void write(T i) throws IOException;

  /**
   * Closes the underlying output stream if needed.
   * 
   * @throws IOException
   */
  void close() throws IOException;
}
