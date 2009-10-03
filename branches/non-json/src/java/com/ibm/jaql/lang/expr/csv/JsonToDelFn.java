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
package com.ibm.jaql.lang.expr.csv;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.RandomAccessBuffer;

/**
 * A function for converting JSON to CSV. It is called as follows:
 * <code>jsonToDel({fields: '...', delimiter: '...'})</code>.
 */
public class JsonToDelFn extends Expr {

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par12 {
    public Descriptor() {
      super("jsonToDel", JsonToDelFn.class);
    }
  }

  public JsonToDelFn(Expr[] exprs) {
    super(exprs);
  }

  @Override
  public JsonValue eval(Context context) throws Exception {
    JsonRecord options = (JsonRecord) exprs[1].eval(context);
    options = options == null ? JsonRecord.EMPTY : options;
    JsonToDel toDel = new JsonToDel(options);

    JsonArray arr = (JsonArray) exprs[0].eval(context);
    int len = (int) arr.count();
    BufferedJsonArray del = new BufferedJsonArray(len);
    for (int i = 0; i < len; i++) {
      RandomAccessBuffer buf = toDel.convert(arr.get(i));
      JsonString line = new JsonString(buf.getBuffer(), buf.size());
      del.set(i, line);
    }
    return del;
  }
}
