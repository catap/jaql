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

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;

/**
 * Superclass for output adapters that take a {@link JsonRecord} for
 * initialization.
 */
public abstract class AbstractOutputAdapter implements OutputAdapter {
  /**
   * IO descriptor.
   */
  protected JsonRecord args;

  /**
   * Location.
   */
  protected String location;

  /**
   * Output options.
   */
  protected JsonRecord options;

  @Override
  public void init(JsonValue v) throws Exception {
    args = (JsonRecord) v;
    location = AdapterStore.getStore().getLocation(args);
    options = AdapterStore.getStore().output.getOption(args);
  }

  @Override
  public void open() throws Exception {
  // nothing
  }

  @Override
  public void close() throws Exception {
  // nothing
  }
}
