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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;

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
  void init(JsonRecord options);
  
  /**
   * Construct the target Item to which the source key K, value V is converted
   * 
   * @return
   */
  JsonValue createTarget();

  /**
   * Import a key K, value V into an Item target (assumed to be constructed
   * using createTarget). This method is invoked for every Hadoop record during
   * the import. If the number of Hadoop records is big, this method's
   * performance will have a big impact on the overall performance of the import
   * from Hadoop records to JSON values.So the implementation of this method
   * needs to be highly efficient.
   * 
   * @param key
   * @param val
   * @param target
   */
  JsonValue convert(K key, V val, JsonValue target);
  
  
  /** Describes the schema of the values produced by {@link #convert(Object, Object, JsonValue)}. 
   * Implementations should provide as much information as possible to facilitate query 
   * optimization. If no information about the schema is known, return 
   * {@link SchemaFactory#anySchema()}.
   * 
   * @return a schema that all values produced by this converter adhere to
   */
  Schema getSchema();
}