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
package com.ibm.jaql.lang.expr.function;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.util.ClassLoaderMgr;

/** An expression that constructs a JSON value for a built-in function */
public class BuiltInExpr extends Expr
{
  public BuiltInExpr(Expr ... exprs)
  {
    super(exprs);
  }
  
  @Override
  public Schema getSchema()
  {
    return SchemaFactory.functionSchema();
  }
  
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    return result;
  }
  
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print(kw("builtin") + "(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(")");
  }
  
  @Override
  public BuiltInFunction eval(Context context) throws Exception
  {
		BuiltInFunctionDescriptor descriptor = getDescriptor(context);
  	return new BuiltInFunction(descriptor);
  }
  
  @SuppressWarnings("unchecked")
  private BuiltInFunctionDescriptor getDescriptor(Context context) throws Exception {
  	String cls = ((JsonString)exprs[0].eval(context)).toString();
		Class<? extends BuiltInFunctionDescriptor> c = 
			(Class<? extends BuiltInFunctionDescriptor>) ClassLoaderMgr.resolveClass(cls);
		return c.newInstance(); 
  }
}
