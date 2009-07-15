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

import com.ibm.jaql.json.schema.Schema;

/** Input adapters are used for providing JSON views over arbitrary data sources. An adapter 
 * provides an iterator over the values in its underlying data source. */
public interface InputAdapter extends Adapter
{

  /** Returns an iterator over the values in the adapter's underlying data sources. */
  ClosableJsonIterator iter() throws Exception;

  /** Describes the schema of the entire underlying data source (not just its individual elements). 
   * This means that the resulting schema should satisfy {@link Schema#isArrayOrNull()}. If nothing
   * is known, return <code>SchemaFactory.arrayOrNullSchema()</code>. */
  public Schema getSchema(); 
}
