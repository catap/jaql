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
 * Import a Hadoop record, composed of a key K and value V, and convert it into a JSON Item
 * 
 * @param <K>
 * @param <V>
 */
public interface KeyValueImport<K,V> {
  
  /**
   * Initialize the importer based on options represented using a JSON record
   * 
   * @param options
   */
  void init(JRecord options);
  
  /**
   * Construct the target Item to which the source key K, value V is converted
   * 
   * @return
   */
  Item createTarget();
  
  /**
   * Import a key K, value V into an Item target (assumed to be constructed using createTarget)
   * 
   * @param key
   * @param val
   * @param tgt
   */
  void convert(K key, V val, Item tgt);
}