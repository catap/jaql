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
import java.util.HashMap;
import java.util.Map;

import com.ibm.jaql.json.type.JsonValue;

/** provides a registry, i.e., a set of (key, value) pairs, and methods for its serialization. 
 * @param <K> type of keys
 * @param <V> types of values
 */
public class Registry<K, V>
{

  private HashMap<K, V>                          map;

  private RegistryFormat<K, V, ? extends JsonValue> fmt;

  /**
   * @param fmt
   */
  public Registry(RegistryFormat<K, V, ? extends JsonValue> fmt)
  {
    this.map = new HashMap<K, V>();
    this.fmt = fmt;
  }

  /**
   * @param key
   * @param value
   */
  public void register(K key, V value)
  {
    map.put(key, value);
  }

  /**
   * @param key
   */
  public void unregister(K key)
  {
    map.remove(key);
  }

  /**
   * @param key
   * @return
   */
  public V get(K key)
  {
    return map.get(key);
  }

  /**
   * @param copy
   */
  // Caution: does not perform a deep copy 
  public void save(HashMap<K, V> copy)
  {
    if (copy == null) return;
    copy.clear();
    for (Map.Entry<K, V> entry : map.entrySet())
    {
      copy.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * @param copy
   */
  public void restore(HashMap<K, V> copy)
  {
    if (copy == null) return;
    map.clear();
    for (Map.Entry<K, V> entry : copy.entrySet())
    {
      map.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * @param istr
   * @throws Exception
   */
  public void readRegistry(InputStream istr) throws Exception
  {
    fmt.readRegistry(istr, this);
  }

  /**
   * @param ostr
   * @throws Exception
   */
  public void writeRegistry(OutputStream ostr) throws Exception
  {
    fmt.writeRegistry(ostr, map.entrySet().iterator());
  }
}
