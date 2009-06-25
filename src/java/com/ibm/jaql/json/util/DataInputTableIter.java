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
package com.ibm.jaql.json.util;

import java.io.DataInput;
import java.io.IOException;

import com.ibm.jaql.json.type.Item;

/**
 * 
 */
public class DataInputTableIter extends Iter
{
  DataInput input;
  Item      item = new Item(); // TODO: memory
  boolean   eof  = false;

  /**
   * @param input
   * @throws IOException
   */
  public DataInputTableIter(DataInput input) throws IOException
  {
    this.input = input;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.util.Iter#next()
   */
  @Override
  public Item next() throws Exception
  {
    if (!eof)
    {
      item.readFields(input);
      if (item.getEncoding() != Item.Encoding.UNKNOWN)
      {
        return item;
      }
      eof = true;
    }
    return null;
  }
}
