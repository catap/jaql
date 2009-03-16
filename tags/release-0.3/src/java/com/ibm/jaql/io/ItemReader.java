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
package com.ibm.jaql.io;

import java.io.IOException;

import com.ibm.jaql.json.type.Item;

/**
 * A reader that creates its return Item and iterates over a stream of Items
 */
public abstract class ItemReader
{

  /**
   * 
   * @return
   */
  public Item createValue()
  {
    return new Item();
  }

  /**
   * 
   * @param value
   * @return
   * @throws IOException
   */
  public abstract boolean next(Item value) throws IOException;

  /**
   * 
   * @throws IOException
   */
  public void close() throws IOException
  {
  }
}
