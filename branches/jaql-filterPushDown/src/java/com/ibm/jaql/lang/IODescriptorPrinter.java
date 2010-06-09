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
package com.ibm.jaql.lang;

import java.io.IOException;

import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;

/**
 * A printer using an IO descriptor. It is used when output options are provided
 * in JAQL shell batch mode.
 */
public class IODescriptorPrinter implements JaqlPrinter {

  private ClosableJsonWriter writer;

  public IODescriptorPrinter(ClosableJsonWriter writer) {
    this.writer = writer;
  }

  @Override
  public void print(Expr expr, Context context) throws Exception {
    JsonIterator it = expr.iter(context);
    if (it != null) {
      for (JsonValue value : it) {
        writer.write(value);
      }
    }
  }

  @Override
  public void printPrompt() throws IOException {}

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
