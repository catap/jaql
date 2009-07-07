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

/**
 * 
 */
public class NotExpr extends Expr
{
  /**
   * @param exprs
   */
  public NotExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public NotExpr(Expr expr)
  {
    super(new Expr[]{expr});
  }

  @Override
  public Schema getSchema()
  {
    if (exprs[0].getSchema().isNull().maybe() || exprs[1].getSchema().isNull().maybe())
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
    exprText.print("not (");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonBool eval(final Context context) throws Exception
  {
    JsonBool b = (JsonBool) exprs[0].eval(context);
    if (b == null)
    {
      return b;
    }
    return JsonBool.make(!b.getValue());
  }
}
