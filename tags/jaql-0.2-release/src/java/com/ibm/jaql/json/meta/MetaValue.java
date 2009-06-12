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
package com.ibm.jaql.json.meta;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** Provides read/write functionality for plain Java objects. Subclasses implement this 
 * functionality for specific Java objects. */
public abstract class MetaValue
{
  /** Create an instance of the JSON structure represented by this MetaValue.
   * @return
   */
  public abstract Object newInstance();

  /** Read the JSON structure represented by this MetaValue from the input 
   * and return it. Reuses the provided instance, if possible.
   * @param in
   * @param obj instance of the type returned by {@link #newInstance()}; must not be null
   * @return
   * @throws IOException
   */
  public abstract Object read(DataInput in, Object obj) throws IOException;
 
  /** Write the provided JSON structure to the output.
   * @param out
   * @param obj
   * @throws IOException
   */
  public abstract void write(DataOutput out, Object obj) throws IOException;
}
