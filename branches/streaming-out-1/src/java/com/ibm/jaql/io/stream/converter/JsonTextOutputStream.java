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
package com.ibm.jaql.io.stream.converter;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;

/**
 * A converter to print JSON value. In array access mode, items of an JSON array
 * are wrapped in a beginning <i>[</i> and an ending <i>]</i>. Items are
 * separated with <i>,</i>. {@link DefaultTextFullSerializer} is for the
 * serialization of JSON values.
 */
public class JsonTextOutputStream extends AbstractJsonTextOutputStream {

  private static String ARR_OPEN = "[";
  private static String ARR_CLOSE = "]";
  private static String ARR_SEP = ",";

  @Override
  public void init(JsonValue options) throws Exception {}

  public JsonTextOutputStream() {
    super(ARR_OPEN, ARR_SEP, ARR_CLOSE);
  }

  @Override
  protected void printValue(PrintStream print, JsonValue i) throws IOException {
    JsonUtil.print(print, i);
  }
}
