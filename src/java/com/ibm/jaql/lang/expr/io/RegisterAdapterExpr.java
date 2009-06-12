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
import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * Register a key, value pair.
 * 
 * registerAdapter({type: string, inOptions: {}, outOptions: {}})
 */
@JaqlFn(fnName = "registerAdapter", minArgs = 1, maxArgs = 1)
public class RegisterAdapterExpr extends Expr
{
  public RegisterAdapterExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonValue rValue = exprs[0].eval(context);
    JsonRecord registry = (JsonRecord) rValue;

    String type = JaqlUtil.enforceNonNull(registry.getValue(Adapter.TYPE_NAME)).toString();
    JsonValue inValue = registry.getValue(Adapter.INOPTIONS_NAME);
    JsonValue outValue = registry.getValue(Adapter.OUTOPTIONS_NAME);
    if (inValue == null && outValue == null)
      throw new RuntimeException("Both input and output options cannot be null");

    JsonRecord inOptions = (JsonRecord) inValue;
    JsonRecord outOptions = (JsonRecord) outValue;

    JaqlUtil.getAdapterStore().register(new JsonString(type),
        new AdapterStore.AdapterRegistry(inOptions, outOptions));

    return rValue;
  }
}
