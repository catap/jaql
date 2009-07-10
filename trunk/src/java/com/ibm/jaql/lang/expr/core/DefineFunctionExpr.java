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
import java.util.Map;

import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JaqlFunction;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;

/**
 * exprs:
 *    BindingExpr(param)
 *    ...
 *    Expr body
 */
public final class DefineFunctionExpr extends Expr
{
  protected static Expr[] makeArgs(Var[] params, Expr body)
  {
//    HashSet<Var> cv = body.getCapturedVars();
//    Expr[] args = new Expr[cv.size() + params.length + 1];
//    int i = 0;
//    for( Var oldVar: cv )
//    {
//      Var newVar = new Var(oldVar.name());
//      args[i] = new BindingExpr(BindingExpr.Type.EQ, newVar, null, new VarExpr(oldVar));
//      body.replaceVar(oldVar, newVar); // TODO: could replace all in one pass with a VarMap
//    }
    Expr[] args = new Expr[params.length + 1];
    for(int i = 0 ; i < params.length ; i++)
    {
      args[i] = new BindingExpr(BindingExpr.Type.EQ, params[i], null, Expr.NO_EXPRS);
    }
    args[args.length-1] = body;
    return args;
  }

  protected static Expr[] makeArgs(ArrayList<Var> params, Expr body)
  {
    return makeArgs(params.toArray(new Var[params.size()]), body);
  }

  /**
   * 
   * @param exprs
   */
  public DefineFunctionExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param params
   * @param body
   */
  public DefineFunctionExpr(Var[] params, Expr body)
  {
    super(makeArgs(params, body));
  }

  /**
   * @param fnVar
   * @param params
   * @param body
   */
  public DefineFunctionExpr(ArrayList<Var> params, Expr body)
  {
    super(makeArgs(params, body));
  }

  /**
   * @return
   */
  public int numParams()
  {
    return exprs.length - 1;
  }
  
  /**
   * @return
   */
  public BindingExpr param(int i)
  {
    assert i < numParams();
    return (BindingExpr)exprs[i];
  }

  /**
   * @return
   */
  public Expr body()
  {
    return exprs[exprs.length-1];
  }

  @Override
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    if (hasCaptures()) {
      result.put(ExprProperty.HAS_CAPTURES, true);
    }
    return result;
  }
  
  /*
   * (non-Javadoc)
   */
  private boolean hasCaptures()
  {
    // TODO: make more efficient
    HashSet<Var> capturedVars = new HashSet<Var>();
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream exprText = new PrintStream(outStream);
    try
    {
      this.decompile(exprText, capturedVars);
      boolean hasCaptures = !capturedVars.isEmpty();
      return hasCaptures;
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
    exprText.print("fn");
    exprText.print("(");
    String sep = "";
    int n = numParams();
    for(int i = 0 ; i < n ; i++)
    {
      BindingExpr b = param(i);
      exprText.print(sep);
      exprText.print(b.var.name);
      sep = ", ";
    }
    exprText.print(") ( ");
    body().decompile(exprText, capturedVars);
    exprText.println(" )");

    for(int i = 0 ; i < n ; i++)
    {
      capturedVars.remove(param(i).var);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    this.annotate(); // TODO: move to init call
    DefineFunctionExpr f = this;
    HashSet<Var> capturedVars = this.getCapturedVars(); // TODO: optimize
    int n = capturedVars.size();
    if( n > 0 )
    {
      // If we have captured variables, we need to evaluate and save their value now.
      // We do this by making new local variables in the function that store the captured values.
      // To add new local variables, we have to define a new function.
      // TODO: is it safe to share f when we don't have captures?
      VarMap varMap = new VarMap();
      for(Var oldVar: capturedVars)
      {
        Var newVar = new Var(oldVar.name());
        varMap.put(oldVar, newVar);
      }
      f = (DefineFunctionExpr)this.clone(varMap);
      Expr[] es = new Expr[n + 1];
      int i = 0;
      for( Var v: capturedVars )
      {
        JsonValue val = JsonUtil.getCopy(v.getValue(context), null);
        es[i++] = new BindingExpr(BindingExpr.Type.EQ, varMap.get(v), null, new ConstExpr(val));
      }
      es[n] = f.body().injectAbove();
      new DoExpr(es);
    }
    JaqlFunction fn = new JaqlFunction(f, n > 0);
    return fn;
  }

  public void annotate()
  {
    int p = numParams();
    if( p == 0 )
    {
      return;
    }
    ArrayList<Expr> uses = new ArrayList<Expr>();
    Expr body = body();
    for(int i = 0 ; i < p ; i++)
    {
      uses.clear();
      BindingExpr b = param(i);
      b.var.usage = Var.Usage.EVAL;
      body.getVarUses(b.var, uses);
      int n = uses.size();
      if( n == 0 )
      {
        b.var.usage = Var.Usage.UNUSED;
      }
      else if( n == 1 )
      {
        Expr e = uses.get(0);
        while( e != body )
        {
          if( e.isEvaluatedOnceByParent().maybeNot() )
          {
            break;
          }
          e = e.parent();
        }
        if( e == body )
        {
          b.var.usage = Var.Usage.STREAM;
        }
      }
    }
  }

}
