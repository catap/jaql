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
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.basic.JsonStringUnquotedSerializer;
import com.ibm.jaql.io.serialization.text.def.DefaultTextFullSerializer;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.SystemUtil;

/**
 * Writes every JSON value in a new line in array access mode.
 * {@link JsonStringUnquotedSerializer} is used for the serialization of JSON
 * string. {@link DefaultTextFullSerializer} is for the serialization of other
 * kinds of JSON values.
 */
public abstract class LinesJsonTextOutputStream extends
                                               AbstractJsonTextOutputStream {
  protected JsonStringUnquotedSerializer strSer = new JsonStringUnquotedSerializer();

  public LinesJsonTextOutputStream() {
    super("", SystemUtil.LINE_SEPARATOR, "");
  }

  @Override
  protected void printValue(PrintStream print, JsonValue i) throws IOException {
    JsonString line = convert(i);
    strSer.write(print, line);
  }

  /**
   * Converts the JSON value into a JSON string.
   * 
   * @param v JSON value
   * @return converted JSON string
   * @throws IOException
   */
  protected abstract JsonString convert(JsonValue v) throws IOException;
}
