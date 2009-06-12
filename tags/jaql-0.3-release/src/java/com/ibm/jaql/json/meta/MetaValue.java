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

/**
 * 
 */
public abstract class MetaValue
{
  /**
   * @return
   */
  public abstract Object newInstance();
  /**
   * @param in
   * @param obj
   * @return
   * @throws IOException
   */
  public abstract Object read(DataInput in, Object obj) throws IOException;
  /**
   * @param out
   * @param obj
   * @throws IOException
   */
  public abstract void write(DataOutput out, Object obj) throws IOException;
}
