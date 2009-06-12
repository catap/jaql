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

import com.ibm.jaql.json.type.JValue;

/**
 * 
 */
public abstract class JIterator
{
  protected JValue current;

  /**
   * 
   */
  public JIterator()
  {
  }

  /**
   * Use this to set the result of current() when known up-front.
   * 
   * @param result
   */
  public JIterator(JValue result)
  {
    current = result;
  }

  /**
   * @return true iff there is a next value.
   */
  public abstract boolean moveNext() throws Exception;

  /**
   * @return
   * @throws Exception
   */
  public final boolean moveNextNonNull() throws Exception
  {
    while (moveNext())
    {
      if (current != null)
      {
        return true;
      }
    }
    return false;
  }

  /**
   * @return
   */
  public final JValue current()
  {
    return current;
  }
}
