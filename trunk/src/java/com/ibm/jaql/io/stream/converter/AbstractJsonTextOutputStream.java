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
import java.io.OutputStream;
import java.io.PrintStream;

import com.ibm.jaql.io.converter.JsonToStream;
import com.ibm.jaql.json.type.JsonValue;

/**
 * A converter to write serialized {@link JsonValue}s to a text output stream.
 */
public abstract class AbstractJsonTextOutputStream implements
    JsonToStream<JsonValue> {

  protected PrintStream output;
  protected boolean arrAcc = true;
  protected boolean seenFirst = false;
  protected boolean close = true;
  protected String start;
  protected String end;
  protected String sep;

  /**
   * Constructs a JSON text output stream.
   * 
   * @param start The string to be written to this output stream before writing
   *          any array items in array access mode.
   * @param sep The string to be written to this output stream between writing
   *          two array items in array access mode.
   * @param end The string to be written to this output stream before cleaning
   *          up this output stream in array access mode.
   */
  public AbstractJsonTextOutputStream(String start, String sep, String end) {
    this.start = start;
    this.sep = sep;
    this.end = end;
  }

  @Override
  public void setOutputStream(OutputStream out) {
    if (out == System.out)
      close = false;
    output = new PrintStream(out);
  }

  @Override
  public void setArrayAccessor(boolean a) {
    arrAcc = a;
  }

  @Override
  public boolean isArrayAccessor() {
    return arrAcc;
  }

  @Override
  public void write(JsonValue i) throws IOException {
    if (seenFirst && !arrAcc)
      throw new RuntimeException("Expected only one value when not in array mode");
    if (!seenFirst && arrAcc) {
      output.print(start);
    }
    if (seenFirst)
      output.print(sep);
    else
      seenFirst = true;
    printValue(output, i);
  }

  @Override
  public void close() throws IOException {
    if (output != null) {
      if (seenFirst && arrAcc)
        output.print(end);
      if (!close)
        output.println();
      output.flush();
      if (close)
        output.close();
    }
  }

  /**
   * Prints the JSON value to the print stream.
   * 
   * @param print A print stream
   * @param i A JSON value
   * @throws IOException
   */
  protected abstract void printValue(PrintStream print, JsonValue i) throws IOException;
}
