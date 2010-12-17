/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.util;

import java.util.Iterator;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;

/** Wraps an {@link Iterator} into a {@link JsonIterator}. */
public class IteratorToJsonIterator extends JsonIterator
{
  protected Iterator<JsonValue> iter;
  
  public IteratorToJsonIterator(Iterator<JsonValue> iter)
  {
    this.iter = iter;
  }

  @Override
protected boolean moveNextRaw() throws Exception
  {
    if( iter.hasNext() )
    {
      currentValue = iter.next();
      return true;
    }
    return false;
  }
}
