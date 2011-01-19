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

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.MacroExpr;

/** An expression that constructs a JSON value for a Java UDF */
/**
 * @jaqlDescription construct a jaql function from a given class
 * Usage:
 * fn javaudf( string className );
 * 
 * The javaudf function constructs a function that knows how to evaluate itself
 * given a className that specifies its body. The function can then be assigned
 * to a variable (like any other value) and invoked (like any other function). 
 * This is the primary means by which users can supply user-defined functions.
 * 
 * @jaqlExample split = javaudf("com.acme.extensions.fn.Split1"); // define the function and assign it to the variable split
   
   @jaqlExample path = '/home/mystuff/stuff';

   @jaqlExample split1(path, "/"); // invoke the split function
 *
 */
public class JavaUdfExpr extends MacroExpr {
	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
	{
	  public Descriptor()
	  {
	    super("javaudf", JavaUdfExpr.class);
	  }	   
	}
  
  public JavaUdfExpr(Expr ... exprs)
  {
    super(exprs);
  }
	
//  @Override
//  public Schema getSchema()
//  {
//    return SchemaFactory.functionSchema();
//  }
//  
//  public Map<ExprProperty, Boolean> getProperties() 
//  {
//    Map<ExprProperty, Boolean> result = super.getProperties();
//    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
//    return result;
//  }
//  
//  @Override
//  public JavaUdfFunction eval(Context context) throws Exception {
//    JavaUdfFunction f = new JavaUdfFunction(((JsonString)exprs[0].eval(context)).toString());
//    return f;
//  }

  /**
   * JavaUdf is a MacroExpr to ensure that it is a literal class at parse time.
   */
  @Override
  public Expr expand(Env env) throws Exception
  {
    Expr e = exprs[0];
    if( !(e instanceof ConstExpr) )
    {
      throw new RuntimeException("javaudf() requires a literal class name");
    }
    ConstExpr ce = (ConstExpr)e;
    JsonString s = (JsonString)ce.value;
    ce.value = new JavaUdfFunction(s.toString());
    return ce;
  }
}
