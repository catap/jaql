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

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;

/**
 * Export a JSON Item and convert it to a Hadoop record, composed of a key K and
 * value V.
 * 
 * @param <K> Type of Hadoop record key
 * @param <V> Type of Hadoop record value
 */
public interface KeyValueExport<K, V> {

  /**
   * Initializes the exporter based on options represented using a JSON record.
   * 
   * @param options
   */
  void init(JsonRecord options);

  /**
   * Constructs the target Key K whose value is from the source JSON Item being
   * converted.
   * 
   * @return
   */
  K createKeyTarget();

  /**
   * Construct the target Value V whose value is from the source JSON Item being
   * converted
   * 
   * @return Hadoop record value
   */
  V createValueTarget();

  /**
   * Export a source JSON value into a target Hadoop Record key K, value V
   * (assumed to be constructed using createTarget).
   * 
   * @param src A JSON value
   * @param key Hadoop record key
   * @param val Hadoop record value
   */
  void convert(JsonValue src, K key, V val);
}