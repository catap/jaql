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

import com.ibm.jaql.json.type.JsonValue;

/** Interface for converters from JSON to some type <code>T</code>.
 * 
 * @param <T> target type of the conversion
 */
public interface FromJson<T>
{

  /** Creates a target JSON value as expected by the {@link #convert} function. */
  T createTarget();

  /** Converts <code>src</code> into type <code>T</code> and returns the result. Implementations 
   * may reuse the <code>target</code> argument to improve efficiency. Callers to this method will 
   * provide either a target value created by {@link #createTarget()} or the result of a previous 
   * call to this method.
   * 
   * @param src the value to convert
   * @param target target value that can be reused
   * @returns the converted value
   */
  T convert(JsonValue src, T target);
}
