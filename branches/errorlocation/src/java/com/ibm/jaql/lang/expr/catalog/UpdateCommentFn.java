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
package com.ibm.jaql.lang.expr.catalog;

import com.ibm.jaql.catalog.Catalog;
import com.ibm.jaql.catalog.CatalogImpl;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * An expression that updates comment field of entry in catalog.
 * 
 * @see CatalogConnection#update(com.ibm.jaql.json.type.JsonRecord,
 *      com.ibm.jaql.json.type.JsonRecord, com.ibm.jaql.json.type.JsonBool)
 */
public class UpdateCommentFn extends Expr {
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11 {
    public Descriptor() {
      super("updateComment", UpdateCommentFn.class);
    }
  }

  public UpdateCommentFn(Expr[] exprs) {
    super(exprs);
  }

  @Override
  protected JsonValue evalRaw(Context context) throws Exception {
	  Catalog cat = new CatalogImpl();
	  cat.open();
	  JsonString key = (JsonString) exprs[0].eval(context);
	  JsonRecord val = (JsonRecord) exprs[1].eval(context);
	  cat.update(key, val, false);
	  return JsonBool.TRUE;
  }
}
