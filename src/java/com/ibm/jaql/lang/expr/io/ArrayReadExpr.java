/*
 * Copyright (C) IBM Corp. 2008.
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
import com.ibm.jaql.io.hadoop.ArrayInputFormat;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;

/**
 * 
 */
@JaqlFn(fnName = "arrayRead", minArgs = 1, maxArgs = 1)
public class ArrayReadExpr extends MacroExpr
{
  /**
   * @param exprs
   */
  public ArrayReadExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.MacroExpr#expand(com.ibm.jaql.lang.core.Env)
   */
  public Expr expand(Env env) throws Exception
  {
    NameValueBinding tField = new NameValueBinding(Adapter.TYPE_NAME,
        new ConstExpr(new JString("array")));
    // FIXME: get rid of the need for a location field
    NameValueBinding lField = new NameValueBinding(Adapter.LOCATION_NAME,
        new ConstExpr(new JString("")));
    NameValueBinding aField = new NameValueBinding(ArrayInputFormat.ARRAY_NAME,
        exprs[0]);
    RecordExpr oRec = new RecordExpr(new Expr[]{aField});
    NameValueBinding oField = new NameValueBinding(Adapter.INOPTIONS_NAME, oRec);
    RecordExpr rec = new RecordExpr(new Expr[]{tField, lField, oField});
    return new StReadExpr(rec);
  }
}
