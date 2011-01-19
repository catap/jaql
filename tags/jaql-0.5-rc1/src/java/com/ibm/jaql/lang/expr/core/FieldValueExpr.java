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

import java.util.HashSet;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.util.FastPrinter;

/**
 * 
 */
public class FieldValueExpr extends Expr
{
  /**
   * expr[0].expr[1]
   * 
   * @param exprs
   */
  public FieldValueExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * rec.name
   * 
   * @param rec
   * @param name
   */
  public FieldValueExpr(Expr rec, Expr name)
  {
    super(rec, name);
  }

  /**
   * $rec.name
   * 
   * @param record
   * @param name
   */
  public FieldValueExpr(Var recVar, String name)
  {
    super(new VarExpr(recVar), new ConstExpr(new JsonString(name)));
  }

  //// TODO: optimize constant name case
  //public FieldValueExpr(String nm, Expr input)
  //{
  //  this(new Expr[]{ input, new ConstExpr(new Item(new JString(nm))), input);
  //}

 
  /**
   * @return
   */
  public final Expr recExpr()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final Expr nameExpr()
  {
    return exprs[1];
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("(");
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
  public JsonValue eval(final Context context) throws Exception
  {
    JsonRecord record = (JsonRecord) exprs[0].eval(context);
    if (record == null)
    {
      return null;
    }
    JsonString name = (JsonString) exprs[1].eval(context); // possible cast error
    if (name == null)
    {
      return null;
    }
    return record.get(name);
  }
}
