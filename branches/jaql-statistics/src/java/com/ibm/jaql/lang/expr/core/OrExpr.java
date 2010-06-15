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
package com.ibm.jaql.lang.expr.core;

import java.io.PrintStream;
import java.util.HashSet;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import static com.ibm.jaql.json.type.JsonType.*;

/**
 * 
 */
public class OrExpr extends Expr
{
  /**
   * @param exprs
   */
  public OrExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr1
   * @param expr2
   */
  public OrExpr(Expr expr1, Expr expr2)
  {
    super(new Expr[]{expr1, expr2});
  }

  @Override
  public Schema getSchema()
  {
    if (exprs[0].getSchema().is(NULL).maybe() || exprs[1].getSchema().is(NULL).maybe())
    {
      return SchemaFactory.booleanOrNullSchema();
    }
    else
    {
      return SchemaFactory.booleanSchema();
    }
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
    exprText.print('(');
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(") " + kw("or") + " (");
    exprs[1].decompile(exprText, capturedVars);
    exprText.print(')');
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonBool eval(final Context context) throws Exception
  {
    JsonBool b1 = (JsonBool) exprs[0].eval(context);
    if (b1 != null && b1.get() == true)
    {
      return JsonBool.TRUE;
    }
    // b1 is false or null
    JsonBool b2 = (JsonBool) exprs[1].eval(context);
    if (b2 == null)
    {
      return null;
    }
    if (b2.get() == true)
    {
      return JsonBool.TRUE;
    }
    // b1 is false or null, b2 is false
    return b1;
  }
}
