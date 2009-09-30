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

import com.ibm.jaql.json.type.JsonString;

/**
 * An interface for accessing a data source.
 */
public interface Adapter extends Initializable {
  JsonString TYPE_NAME = new JsonString("type");

  JsonString LOCATION_NAME = new JsonString("location");

  JsonString INOPTIONS_NAME = new JsonString("inoptions");

  JsonString OUTOPTIONS_NAME = new JsonString("outoptions");

  JsonString OPTIONS_NAME = new JsonString("options");

  JsonString ADAPTER_NAME = new JsonString("adapter");

  JsonString FORMAT_NAME = new JsonString("format");

  JsonString CONVERTER_NAME = new JsonString("converter");

  /**
   * Once an adapter has been initialized, you can take whatever steps that are
   * needed prior to accessing the data. This is called by expressions, but not
   * MapReduce.
   * 
   * @throws Exception
   */
  void open() throws Exception;

  /**
   * After accessing the data, clean up. This is called by expressions, but not
   * MapReduce.
   * 
   * @throws Exception
   */
  void close() throws Exception;
}
