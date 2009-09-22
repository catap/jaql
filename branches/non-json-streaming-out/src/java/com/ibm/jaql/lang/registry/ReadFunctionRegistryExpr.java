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
package com.ibm.jaql.lang.registry;

import com.ibm.jaql.io.registry.RegistryUtil;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
@JaqlFn(fnName = "readFunctionRegistry", minArgs = 1, maxArgs = 1)
public class ReadFunctionRegistryExpr extends Expr
{
  /**
   * @param exprs
   */
  public ReadFunctionRegistryExpr(Expr[] exprs)
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
    JsonString fileName = JaqlUtil.enforceNonNull((JsonString)exprs[0].eval(context));

    RegistryUtil.readFile(fileName.toString(), JaqlUtil.getFunctionStore()); // FIXME: memory
    return fileName;
  }
}
