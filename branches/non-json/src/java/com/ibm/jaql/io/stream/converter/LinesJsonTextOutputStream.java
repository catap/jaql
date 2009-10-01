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
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;

/**
 * Writes every JSON value in a new line. {@link JsonStringUnquotedSerializer}
 * is used for the serialization of JSON string.
 * {@link DefaultTextFullSerializer} is for the serialization of other kinds of
 * JSON values.
 */
public class LinesJsonTextOutputStream extends AbstractJsonTextOutputStream {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  private JsonStringUnquotedSerializer strSer = new JsonStringUnquotedSerializer();

  public LinesJsonTextOutputStream() {
    super("", LINE_SEPARATOR, "");
  }

  @Override
  protected void printValue(PrintStream print, JsonValue i) throws IOException {
    if (i instanceof JsonString)
      strSer.write(print, i);
    else
      JsonUtil.print(print, i);
  }
}
