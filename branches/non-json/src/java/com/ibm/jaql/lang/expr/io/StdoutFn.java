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
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

public class StdoutFn extends AbstractHandleFn {

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par01 {
    public Descriptor() {
      super(STDOUT, StdoutFn.class);
    }
  }

  public static final String STDOUT = "stdout";
  private final static JsonValue TYPE = new JsonString(STDOUT);

  /**
   * exprs[0]: options
   * 
   * @param exprs
   */
  public StdoutFn(Expr[] exprs) {
    super(exprs);
  }

  @Override
  protected JsonValue getType() {
    return TYPE;
  }

  @Override
  public boolean isMapReducible() {
    return false;
  }

  @Override
  public JsonRecord eval(Context context) throws Exception {
    BufferedJsonRecord rec = new BufferedJsonRecord();
    rec.add(Adapter.TYPE_NAME, getType());
    JsonValue inout = exprs.length > 0 ? exprs[0].eval(context) : null;
    if (inout != null) {
      rec.add(Adapter.OPTIONS_NAME, inout);
    }
    return rec;
  }
}
