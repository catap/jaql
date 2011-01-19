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
import java.util.Map;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.FastPrinter;

/**
 * FieldExpr is not really an Expr. It is only used inside a RecordExpr.
 */
public abstract class FieldExpr extends Expr
{
  /**
   * @param exprs
   */
  public FieldExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   * @param expr
   */
  public FieldExpr(Expr expr)
  {
    super(expr);
  }

  /**
   * 
   * @param expr0
   * @param expr1
   */
  public FieldExpr(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }

  @Override
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public abstract void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception;
  
  /**
   * @param context
   * @param rec
   * @throws Exception
   */
  public abstract void eval(Context context, BufferedJsonRecord rec)
      throws Exception;
  
  /**
   * @param name
   * @return
   */
  public abstract Bool3 staticNameMatches(JsonString name);

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception
  {
    throw new RuntimeException("FieldExpr should never be evaluated!");
  }
}
