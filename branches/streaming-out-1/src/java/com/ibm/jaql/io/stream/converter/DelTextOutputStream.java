/*
 * Copyright (C) IBM Corp. 2009.
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

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.del.JsonToDel;
/**
 * A converter to convert a JSON value into a CSV line.
 */
public class DelTextOutputStream extends LinesJsonTextOutputStream {
  
  private JsonToDel toDel;
  
  @Override
  public void init(JsonValue options) throws Exception {
    toDel = new JsonToDel((JsonRecord) options);
  }
  
  /**
   * Converts the JSON value into a JSON string for a line in a CSV file.
   */
  @Override
  protected JsonString convert(JsonValue v) throws IOException {
    return toDel.convertToJsonString(v);
  }
}
