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
package com.ibm.jaql.io.hadoop.converter;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.del.JsonToDel;
import com.ibm.jaql.util.RandomAccessBuffer;

/** Converts JSON array or record to a delimited text value. */
public final class ToDelConverter implements KeyValueExport<NullWritable, Text> {

  private JsonToDel toDel;

  @Override
  public void init(JsonRecord options) {
    toDel = new JsonToDel(options);
  }

  @Override
  public NullWritable createKeyTarget() {
    return null;
  }

  @Override
  public Text createValueTarget() {
    return new Text();
  }

  /**
   * Converts the given line into a JSON value.
   * 
   * @throws IOException
   */
  @Override
  public void convert(JsonValue src, NullWritable key, Text text) {
    try {
      RandomAccessBuffer buf = toDel.convert(src);
      text.set(buf.getBuffer(), 0, buf.size());
    } catch (Exception e) {
      throw new UndeclaredThrowableException(e);
    }
  }
}