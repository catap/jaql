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
package com.ibm.jaql.lang.expr.top;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;

/**
 * 
 */
public class MaterializeExpr extends TopExpr
{
  private Var var;

  /**
   * materialize var = expr;
   * 
   * @param var
   * @param expr
   */
  public MaterializeExpr(Env env, Var var, Expr expr)
  {
    super(env, new Expr[]{expr.clone(new VarMap())});
    this.var = var;
    var.setExpr(expr);
  }

  /**
   * materialize var;  
   * 
   * ie, materialize var = var.expr
   * 
   * @param var
   */
  public MaterializeExpr(Env env, Var var)
  {
    this(env, var, var.expr());
  }

  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = ExprProperty.createUnsafeDefaults();
    result.put(ExprProperty.HAS_CAPTURES, true);
    return result;
  }

  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars) throws Exception
  {
    assert var.isGlobal();
    exprText.print(kw("materialize") + " ");
    exprText.print("::" + var.taggedName());
//    exprText.print(" = ");
//    exprs[0].decompile(exprText, capturedVars);
  }
  
  @Override
  public Expr clone(VarMap varMap)
  {
    return new MaterializeExpr(env, var, exprs[0].clone(varMap));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonString eval(Context context) throws Exception
  {
//    JsonBool result = JsonBool.FALSE;
    if( var.type() != Var.Type.VALUE )
    {
//      result = JsonBool.TRUE;
      JsonValue value = env.eval(exprs[0]);
      var.setValue(value);
    }
    return new JsonString(var.taggedName());
  }
}
