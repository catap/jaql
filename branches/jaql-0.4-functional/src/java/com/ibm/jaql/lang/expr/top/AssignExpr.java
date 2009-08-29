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
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;

/**
 * 
 */
public class AssignExpr extends TopExpr
{
  public Var var;

  /**
   * @param varName
   * @param valExpr
   */
  public AssignExpr(Var var, Expr valExpr)
  {
    super(new Expr[]{valExpr});
    this.var = var;
  }

  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = ExprProperty.createUnsafeDefaults();
    result.put(ExprProperty.HAS_CAPTURES, true);
    return result;
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
    exprText.print(var.name()); // TODO: expr -> $var when var is pipe var
    exprText.print(" = ");
    exprs[0].decompile(exprText, capturedVars);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception
  {
    var.expr = exprs[0];
    var.isDefined = true;
    return new JsonString(var.name());
//    var.expr = exprs[0]; // TODO: hack: this is just signalling to use the value
//    var.value = new Item(); // TODO: memory
//    var.value.copy(exprs[0].eval(context)); // TODO: need deferred evaluation for top var defs; 
//    // TODO: we can avoid copy when this is a top assignment and we own the entire expr tree (so nobody reevals) 
//    return Item.nil;
  }
}
