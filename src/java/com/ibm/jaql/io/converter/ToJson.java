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
package com.ibm.jaql.io.converter;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;

/** Interface for converters from <code>T</code> to JSON.
 * 
 * @param <T> type of input values
 */
public interface ToJson<T>
{
  /** Creates a target JSON value as expected by the {@link #convert} function. */
  JsonValue createTarget();

  /** Converts <code>src</code> into a JSON value and returns the result. Implementations may 
   * reuse the <code>target</code> argument to improve efficiency. Callers to this method will 
   * provide either a target value created by {@link #createTarget()} or the result of a previous 
   * call to this method.
   * 
   * @param src the value to convert
   * @param tgt target value that can be reused
   * @returns the converted value
   */
  JsonValue convert(T src, JsonValue target);
  
  /** Describes the schema of the values produced by {@link #convert(JsonValue, Object)}. 
   * Implementations should provide as much information as possible to facilitate query 
   * optimization. If no information about the schema is known, return 
   * {@link SchemaFactory#anyOrNullSchema()}.
   * 
   * @return a schema that all values produced by this converter adhere to
   */
  Schema getSchema();
}
