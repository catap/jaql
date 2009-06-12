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

/** An empty iterator. */
public final class EmptyIterator extends JsonIterator
{
  private static final EmptyIterator THE_INSTANCE = new EmptyIterator();
  
  private EmptyIterator() { };
  
  public static EmptyIterator getInstance() {
    return THE_INSTANCE;
  }

  /** Returns <code>false</code>. */
  public boolean moveNext()
  {
    return false;
  }
}
