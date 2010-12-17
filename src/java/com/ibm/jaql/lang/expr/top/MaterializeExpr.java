// TODO: remove dead code
///*
// * Copyright (C) IBM Corp. 2008.
// * 
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not
// * use this file except in compliance with the License. You may obtain a copy of
// * the License at
// * 
// * http://www.apache.org/licenses/LICENSE-2.0
// * 
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// * License for the specific language governing permissions and limitations under
// * the License.
// */
//package com.ibm.jaql.lang.expr.top;
//
//import java.io.PrintStream;
//import java.util.HashSet;
//import java.util.Map;
//
//import com.ibm.jaql.json.type.JsonString;
//import com.ibm.jaql.json.type.JsonValue;
//import com.ibm.jaql.lang.core.Context;
//import com.ibm.jaql.lang.core.Var;
//import com.ibm.jaql.lang.core.VarMap;
//import com.ibm.jaql.lang.expr.core.Expr;
//import com.ibm.jaql.lang.expr.core.ExprProperty;
//
///**
// * 
// */
//public class MaterializeExpr extends Expr
//{
//  private Var var;
//
//  /**
//   * materialize var = expr;
//   * 
//   * @param var
//   * @param expr
//   */
//  public MaterializeExpr(Var var, Expr expr)
//  {
//    super(expr);
//    this.var = var;
//  }
//
//  public Map<ExprProperty, Boolean> getProperties()
//  {
//    Map<ExprProperty, Boolean> result = ExprProperty.createUnsafeDefaults();
//    result.put(ExprProperty.HAS_CAPTURES, true); // Not really a capture, but a global var to be set.
//    result.put(ExprProperty.HAS_SIDE_EFFECTS, true);
//    return result;
//  }
//
//  @Override
//  public void decompile(PrintStream exprText, HashSet<Var> capturedVars) throws Exception
//  {
//    assert var.isGlobal();
//    exprText.print(kw("materialize") + " ");
//    exprText.print("::" + var.taggedName());
//    exprText.print(" = ");
//    exprs[0].decompile(exprText, capturedVars,emitLocation);
//  }
//  
//  @Override
//  public Expr clone(VarMap varMap)
//  {
//    return new MaterializeExpr(var, exprs[0].clone(varMap));
//  }
//
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
//   */
//  protected JsonString evalRaw(Context context) throws Exception
//  {
//    JsonValue value = exprs[0].eval(context);
//    // FIXME: this check should be in setValue, 
//    // and we should add another setValueUnchecked when the schema is known safe.
//    if( !var.getSchema().matches(value) )
//    {
//      throw new ClassCastException("cannot assign "+value+" to variable "+var.name()
//          +" with "+var.getSchema());
//    }
//    var.setValue(value);
//    return new JsonString(var.taggedName());
//  }
//}
