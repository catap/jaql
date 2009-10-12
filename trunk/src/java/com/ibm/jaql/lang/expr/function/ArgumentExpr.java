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

import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.util.Bool3;

/** Converts arguments of a function into a record of (argument name, value) pairs */
public class ArgumentExpr extends Expr
{
  private BufferedJsonRecord target;
  private JsonValueParameters pars;

  public ArgumentExpr(Expr[] e) 
  { 
    // this is just to make the other constructor work
    throw new IllegalStateException("constructor must not be used");
  };
  
  public ArgumentExpr(JsonValueParameters pars, List<Expr> positionalArgs, Map<JsonString, Expr> namedArgs)
  {
    // create a dummy built in function to parse the arguments
    BuiltInFunctionDescriptor d = new DefaultBuiltInFunctionDescriptor("dummy", ArgumentExpr.class, pars, null);
    BuiltInFunction f = new BuiltInFunction(d);
    Expr[] exprs = FunctionCallExpr.makeExprs(null, positionalArgs, namedArgs);
    f.setArguments(exprs, 1, exprs.length-1, true);
    this.exprs = f.getArguments();
    
    // initialize rest of variables
    this.pars = pars;
    int n = pars.numParameters();
    target = new BufferedJsonRecord(n);
    for (int i=0; i<n; i++)
    {
      target.add(pars.nameOf(i), null);
    }    
  }
  
  public JsonRecord constEval(Env env) 
  {
    if( ! isCompileTimeComputable().always() )
    {
      throw new RuntimeException("arguments have to be constants");
    }
    try
    {
      return (JsonRecord)(env.eval(this));
    }
    catch (RuntimeException ex)
    {
      throw ex;
    }
    catch( Exception ex )
    {
      throw new UndeclaredThrowableException(ex);
    }  
  }
  
  public Bool3 evaluatesChildOnce()
  {
    return Bool3.TRUE;
  }
  
  @Override
  public JsonRecord eval(Context context) throws Exception
  {
    // construct the function argument
   for (int i=0; i<exprs.length; i++)
   {
     JsonValue v = exprs[i].eval(context);
     if (!pars.get(i).getSchema().matches(v))
     {
       throw new IllegalArgumentException("argument \"" + pars.get(i).getName() + 
           "\" has an invalid schema");
     }
     target.set(i, v);
   }
   return target;    
  }

  @Override
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    return result;
  }
}
