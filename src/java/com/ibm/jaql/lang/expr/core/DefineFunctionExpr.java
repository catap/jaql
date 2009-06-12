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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;

/**
 * 
 */
public class DefineFunctionExpr extends Expr
{
  Var   fnVar; // TODO: use Binding
  Var[] params; // TODO: use Binding
  // Function fn;

  /**
   * @param fnVar
   * @param params
   * @param body
   */
  public DefineFunctionExpr(Var fnVar, Var[] params, Expr body)
  {
    super(new Expr[]{body});
    this.fnVar = fnVar;
    this.params = params;
    //    fn = new Function();
    //    fn.def.fnVar = fnVar;
    //    fn.def.params = params;
    //    fn.setBody(body);
  }

  /**
   * @param fnVar
   * @param params
   * @param body
   */
  public DefineFunctionExpr(Var fnVar, ArrayList<Var> params, Expr body)
  {
    this(fnVar, params.toArray(new Var[params.size()]), body);
  }

  //  public FunctionExpr(Env env, String fnName, ArrayList<String> paramNames)
  //  {
  //    super(NO_EXPRS);
  //    fn = new Function();
  //    int n = paramNames.size();
  //    fn.def.params = new Var[n];
  //    for( int i = 0 ; i < n ; i++ )
  //    {
  //      String p = paramNames.get(i);
  //      fn.def.params[i] = env.scope(p);
  //    }
  //    if( fnName != null )
  //    {
  //      fn.def.fnVar = env.scope(fnName);
  //    }
  //  }
  //
  //  public void setBody(Env env, Expr body, boolean unscopeFn)
  //  {
  //    if( fn.def.fnVar != null && unscopeFn )
  //    {
  //      env.unscope(fn.def.fnVar);
  //    }
  //    for (Var p : fn.def.params)
  //    {
  //      env.unscope(p);
  //    }
  //    fn.setBody(body);
  //  }

  /**
   * @return
   */
  public final Expr body()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final Var[] params()
  {
    return params;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isConst()
   */
  @Override
  public boolean isConst()
  {
    // TODO: make more efficient
    HashSet<Var> capturedVars = new HashSet<Var>();
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream exprText = new PrintStream(outStream);
    try
    {
      this.decompile(exprText, capturedVars);
      boolean noCaptures = capturedVars.isEmpty();
      return noCaptures;
    }
    catch (Exception ex)
    {
      return false;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    //    for (Var c : fn.def.capturedVars)
    //    {
    //      capturedVars.add(c);
    //    }
    //    fn.print(exprText);
    exprText.print("fn");
    if (fnVar != null)
    {
      exprText.print(' ');
      exprText.print(fnVar.name);
    }
    exprText.print("(");
    String sep = "";
    for (Var v : params)
    {
      exprText.print(sep);
      exprText.print(v.name);
      sep = ", ";
    }
    exprText.print(") ( ");
    exprs[0].decompile(exprText, capturedVars);
    exprText.println(" )");

    if (fnVar != null)
    {
      capturedVars.remove(fnVar);
    }
    for (Var v : params)
    {
      capturedVars.remove(v);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  public DefineFunctionExpr clone(VarMap varMap)
  {
    Var[] p = new Var[params.length];
    for (int i = 0; i < p.length; i++)
    {
      p[i] = varMap.remap(params[i]);
    }
    return new DefineFunctionExpr(varMap.remap(fnVar), p, exprs[0]
        .clone(varMap));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Item eval(Context context) throws Exception
  {
    // TODO: memory, wasted overhead setting body
    JFunction fn = new JFunction(fnVar, params, exprs[0]);
    fn.capture(context);
    return new Item(fn); // TODO: figure out how to cache items...
  }

}
