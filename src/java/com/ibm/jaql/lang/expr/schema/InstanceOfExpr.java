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

import java.io.PrintStream;
import java.util.HashSet;

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.Expr;

/**
 * 
 */
public class InstanceOfExpr extends Expr
{
  /**
   * @param exprs
   */
  public InstanceOfExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   * @param schema
   */
  public InstanceOfExpr(Expr expr, Expr schema)
  {
    super(new Expr[]{expr, schema});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  public InstanceOfExpr clone(VarMap varMap)
  {
    return new InstanceOfExpr(cloneChildren(varMap));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(") instanceof (");
    exprs[1].decompile(exprText, capturedVars);
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonBool eval(final Context context) throws Exception
  {
    JsonValue value = exprs[0].eval(context);
    JsonSchema schema = (JsonSchema) exprs[1].eval(context);
    if (schema == null)
    {
      return null;
    }
    boolean b = schema.getSchema().matches(value);
    return JsonBool.make(b);
  }
}
