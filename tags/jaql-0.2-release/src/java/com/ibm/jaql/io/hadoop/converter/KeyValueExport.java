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
package com.ibm.jaql.io.hadoop.converter;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;

/**
 * Export a JSON Item and convert it to a Hadoop record, composed of a key K and value V
 * 
 * @param <K>
 * @param <V>
 */
public interface KeyValueExport<K, V> {
  
  /**
   * Initialize the exporter based on options represented using a JSON record
   * 
   * @param options
   */
  void init(JRecord options);
  
  /**
   * Construct the target Key K from which values from the source JSON Item is converted
   * 
   * @return
   */
  K createKeyTarget();
  
  /**
   * Construct the target Value V from which values from the source JSON Item is converted
   * 
   * @return
   */
  V createValTarget();
  
  /**
   * Export a source Item into a target Hadoop Record key K, value V (assumed to be constructed using createTarget)
   * 
   * @param src
   * @param key
   * @param val
   */
  void convert(Item src, K key, V val);
}