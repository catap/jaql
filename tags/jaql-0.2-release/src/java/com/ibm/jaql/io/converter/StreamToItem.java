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

import com.ibm.jaql.json.type.Item;

/** 
 * Interface for reading {@link Item}s from an {@link InputStream}.
 */
public interface StreamToItem
{

  /**
   * Create an Item into which JSON values are read into.
   * 
   * @return
   */
  Item createTarget();

  /**
   * Set the input stream.
   * 
   * @param in
   */
  void setInputStream(InputStream in);
  
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

  /**
   * Read from the stream into the Item.
   * 
   * @param v
   * @return
   * @throws IOException
   */
  boolean read(Item v) throws IOException;
}
