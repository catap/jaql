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
package com.ibm.jaql.lang.expr.io;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.converter.JsonToStream;
import com.ibm.jaql.io.stream.converter.JsonToStreamConverter;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;

/**
 * An expression that constructs a output format.
 */
public abstract class AbstractOutputFormatFn extends Expr {

  private final Class<? extends JsonToStream<JsonValue>> format;
  private final Class<? extends JsonToStreamConverter> converter;

  /**
   * Constructs a JSON output format.
   * 
   * @param exprs expressions
   * @param format A formatter
   * @param converter A converter
   */
  public AbstractOutputFormatFn(Expr[] exprs,
                                Class<? extends JsonToStream<JsonValue>> format,
                                Class<? extends JsonToStreamConverter> converter) {
    super(exprs);
    this.format = format;
    this.converter = converter;
  }

  @Override
  public JsonValue eval(Context context) throws Exception {
    BufferedJsonRecord rec = new BufferedJsonRecord();
    rec.add(Adapter.FORMAT_NAME, getOption(format));
    rec.add(Adapter.CONVERTER_NAME, getOption(converter));
    return rec;
  }

  private JsonString getOption(Class<?> cls) {
    return new JsonString(cls.getName());
  }
}
