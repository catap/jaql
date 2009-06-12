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
package com.ibm.jaql.io.registry;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

import com.ibm.jaql.json.type.JValue;

/** Interface for serialization of a registry.
 * 
 * @param <K> keys in the registry
 * @param <V> values in the registry
 * @param <E> type external representation of a registry entry
 */
public interface RegistryFormat<K, V, E extends JValue>
{

  /**
   * @param key
   * @param value
   * @return
   */
  E convert(K key, V value);

  /**
   * @param external
   * @return
   */
  K convertKey(E external);

  /**
   * @param external
   * @return
   */
  V convertVal(E external);

  /**
   * @param istr
   * @param registry
   * @throws Exception
   */
  void readRegistry(InputStream istr, Registry<K, V> registry) throws Exception;

  /**
   * @param ostr
   * @param iter
   * @throws Exception
   */
  void writeRegistry(OutputStream ostr, Iterator<Map.Entry<K, V>> iter)
      throws Exception;
}
