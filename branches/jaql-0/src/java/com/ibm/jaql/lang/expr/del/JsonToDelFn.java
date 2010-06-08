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

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * A function for converting JSON to CSV. It is called as follows:
 * <p>
 * <code>jsonToDel({schema: '...', delimiter: '...'})</code> .
 */
public class JsonToDelFn extends IterExpr {

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par12 {
    public Descriptor() {
      super("jsonToDel", JsonToDelFn.class);
    }
  }

  private JsonToDel toDel;

  public JsonToDelFn(Expr[] exprs) {
    super(exprs);
  }

  @Override
  public JsonIterator iter(Context context) throws Exception {
    initToDel(context);
    final JsonIterator it = exprs[0].iter(context);
    return toDel.convertToJsonIterator(it);
  }

  private void initToDel(Context context) throws Exception {
    JsonRecord options = (JsonRecord) exprs[1].eval(context);
    toDel = new JsonToDel(options);
  }
}
