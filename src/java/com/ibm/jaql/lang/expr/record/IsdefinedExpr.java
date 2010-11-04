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
package com.ibm.jaql.lang.expr.record;

import java.util.HashSet;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.util.FastPrinter;

import static com.ibm.jaql.json.type.JsonType.*;

/**
 * 
 */
// @JaqlFn(fnName = "hasField", minArgs = 2, maxArgs = 2)
public final class IsdefinedExpr extends Expr
{
  /**
   * Expr record
   * Expr name
   * 
   * @param exprs
   */
  public IsdefinedExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param recExpr
   */
  public IsdefinedExpr(Expr recExpr, Expr name)
  {
    super(recExpr, name);
  }

  public IsdefinedExpr(Var recVar, String name)
  {
    super(new VarExpr(recVar), new ConstExpr(new JsonString(name)));
  }

  public Schema getSchema()
  {
    if (exprs[0].getSchema().is(NULL).or(exprs[1].getSchema().is(NULL)).maybe())
    {
      return SchemaFactory.booleanOrNullSchema();
    }
    else
    {
      return SchemaFactory.booleanSchema();
    }
  }
  
  @Override
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print(kw("isdefined") + " (");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(").(");
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
    JsonRecord rec = (JsonRecord) exprs[0].eval(context);
    if (rec == null)
    {
      return null;
    }
    JsonString name = (JsonString) exprs[1].eval(context);
    if (name == null)
    {
      return null;
    }
    return JsonBool.make(rec.containsKey(name));
  }

}
