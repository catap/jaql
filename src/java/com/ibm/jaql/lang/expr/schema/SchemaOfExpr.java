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
package com.ibm.jaql.lang.expr.schema;

import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
@JaqlFn(fnName = "schemaof", minArgs = 1, maxArgs = 1)
public class SchemaOfExpr extends Expr
{
  /**
   * @param exprs
   */
  public SchemaOfExpr(Expr[] exprs)
  {
    super(exprs);
  }


  /**
   * schemaof(e) never evaluates e
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonSchema eval(final Context context) throws Exception
  {
    Expr expr = exprs[0];
    return new JsonSchema(expr.getSchema());
  }

}
