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
package com.ibm.jaql.lang.expr.del;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.io.converter.AbstractFromDelConverter;
import com.ibm.jaql.io.serialization.text.basic.BasicTextFullSerializer;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.RandomAccessBuffer;

/**
 * For converting a JSON value to a CSV line.
 */
public class JsonToDel {

  private JsonString[] fields;
  private String delimiter;
  private RandomAccessBuffer buf = new RandomAccessBuffer();
  private PrintStream out = new PrintStream(buf);
  private final BasicTextFullSerializer serializer = new BasicTextFullSerializer();

  /**
   * Constructs a converter with the given initialization options.
   * 
   * @param options Initialization options.
   */
  public JsonToDel(JsonRecord options) {
    JsonString js = (JsonString) options.get(AbstractFromDelConverter.DELIMITER_NAME);
    delimiter = (js == null) ? "," : js.toString();
    JsonArray fieldsArr = (JsonArray) options.get(AbstractFromDelConverter.FIELDS_NAME);
    if (fieldsArr == null) {
      fields = null;
    } else {
      try {
        int n = (int) fieldsArr.count();
        fields = new JsonString[n];
        for (int i = 0; i < n; i++) {
          fields[i] = (JsonString) fieldsArr.get(i);
        }
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e);
      }
    }
  }

  /**
   * Converts the given JSON value.
   * 
   * @param src JSON value
   * @return The buffer contains the byte array of UTF-8 string.
   * @throws IOException
   * @throws IllegalArgumentException If the JSON value is neither a JSON record
   *           nor a JSON array.
   */
  public RandomAccessBuffer convert(JsonValue src) throws IOException {
    buf.reset();
    String sep = "";
    if (src instanceof JsonRecord) {
      JsonRecord rec = (JsonRecord) src;
      // fields are required for records to define order
      for (JsonString n : fields) {
        out.print(sep);
        JsonValue value = rec.get(n);
        serializer.write(out, value);
        sep = delimiter;
      }
    } else if (src instanceof JsonArray) {
      JsonArray arr = (JsonArray) src;
      for (JsonValue value : arr) {
        out.print(sep);
        serializer.write(out, value);
        sep = delimiter;
      }
    } else {
      throw new IllegalArgumentException("Type cannot be placed in delimited file.  Array or record expected: "
          + src);
    }
    out.flush();
    return buf;
  }
}
