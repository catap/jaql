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

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.xml.JsonToXmlFn;
import com.ibm.jaql.util.RandomAccessBuffer;
/**
 * A converter to convert a JSON value to a XML file.
 */
public class XmlTextOutputStream extends LinesJsonTextOutputStream {
  private JsonToXmlFn toXml = new JsonToXmlFn();
  private RandomAccessBuffer buf = new RandomAccessBuffer();
  private PrintStream out = new PrintStream(buf);

  @Override
  public void init(JsonValue options) throws Exception {}

  /**
   * Converts the JSON value into a JSON string for a XML file.
   */
  @Override
  protected JsonString convert(JsonValue v) throws IOException {
    try {
      buf.reset();
      JsonArray arr = toXml.toJsonArray(v);
      String beginNewLine = "";
      for (int i = 0; i < arr.count(); i++) {
        out.print(beginNewLine);
        strSer.write(out, (JsonString) arr.get(i));
        beginNewLine = LINE_SEPARATOR;
      }
      return new JsonString(buf.getBuffer(), buf.size());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
