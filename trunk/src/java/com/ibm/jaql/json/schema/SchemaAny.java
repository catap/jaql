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
package com.ibm.jaql.json.schema;

import java.io.DataOutput;
import java.io.IOException;

import com.ibm.jaql.json.type.Item;

/** Schema that matches everything.
 * 
 */
public class SchemaAny extends Schema
{
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#matches(com.ibm.jaql.json.type.Item)
   */
  @Override
  public boolean matches(Item item)
  {
    return true;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#toString()
   */
  @Override
  public String toString()
  {
    return "*";
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeByte(ANY_TYPE);
  }
}
