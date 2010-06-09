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

import java.io.Closeable;
import java.io.IOException;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;

/**
 * A closable iterator meant for reading data. 
 */
public abstract class ClosableJsonIterator extends JsonIterator implements Closeable
{
  public ClosableJsonIterator() {
  }
  
  public ClosableJsonIterator(JsonValue initialValue) {
    currentValue = initialValue;
  }
  
  @Override
  public void close() throws IOException
  {
    // nothing by default
  }
}
