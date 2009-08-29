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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;

public final class CaptureExpr extends Expr
{
  private final JaqlFunction f;
  
  protected static VarParameters makeParameters(Var[] params, Expr[] defaults)
  {
    assert params.length == defaults.length;
    VarParameter[] pars = new VarParameter[params.length];
    for(int i = 0 ; i < params.length ; i++)
    {
      BindingExpr binding = new BindingExpr(BindingExpr.Type.EQ, params[i], null, Expr.NO_EXPRS);
      if (defaults[i] == null)
      {
        pars[i] = new VarParameter(binding);
      }
      else
      {
        pars[i] = new VarParameter(binding, defaults[i]);
      }
    }
    return new VarParameters(pars);
  }

  protected static VarParameters makeParameters(List<Var> params, List<Expr> defaults)
  {
    return makeParameters(params.toArray(new Var[params.size()]), 
        defaults.toArray(new Expr[defaults.size()]));
  }

  public CaptureExpr(Var[] params, Expr body)
  {
    this(params, new Expr[params.length], body);
  }
  
  /**
   * @param params
   * @param body
   */
  public CaptureExpr(Var[] params, Expr[] defaults, Expr body)
  {
    this.f = new JaqlFunction(makeParameters(params, defaults), body);
    annotate();
  }

  /**
   * @param fnVar
   * @param params
   * @param body
   */
  public CaptureExpr(List<Var> params, List<Expr> defaults, Expr body)
  {
    this.f = new JaqlFunction(makeParameters(params, defaults), body);
    annotate(); 
  }
  
  public CaptureExpr(List<Var> params, Expr body)
  {
    this(params, emptyList(params), body);
  }
  
  public CaptureExpr(JaqlFunction f)
  {
    this.f = f;
    annotate(); 
  }

  private static List<Expr> emptyList(List<Var> var) 
  {
    List<Expr> l = new ArrayList<Expr>(var.size());
    for (int i=0; i<var.size(); i++) l.add(null);
    return l;
  }
  

  /**
   * @return
   */
  public JaqlFunction getFunction()
  {
    return f;
  }

  @Override
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    if (f.hasCaptures()) {
      result.put(ExprProperty.HAS_CAPTURES, true);
    }
    else
    {
      result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    }
    return result;
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
    exprText.print(f.getText());
    VarParameters pars = f.getParameters();
    for(int i = 0 ; i < pars.noParameters(); i++)
    {
      capturedVars.remove(pars.get(i).getVar());
    }
  }
  
  public HashSet<Var> getCapturedVars()
  {
    return f.getCapturedVars();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JaqlFunction eval(Context context) throws Exception
  {
    JaqlFunction f = this.f;
    HashSet<Var> capturedVars = getCapturedVars();
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
        Var newVar = new Var(oldVar.name(), oldVar.getSchema());
        varMap.put(oldVar, newVar);
      }
      
      // clone parameters and body
      VarParameter[] newPars = cloneParameters(varMap);
      Expr newBody = f.body().clone(varMap);
      
      // capture variables
      Expr[] es = new Expr[n + 1];
      int i = 0;
      for( Var v: capturedVars )
      {
        JsonValue val = JsonUtil.getCopy(v.getValue(context), null);
        es[i++] = new BindingExpr(BindingExpr.Type.EQ, varMap.get(v), null, new ConstExpr(val));
      }
      es[n] = newBody;
      
      // create function
      f = new JaqlFunction(new VarParameters(newPars), new DoExpr(es));
    }
    return f;
  }

  private VarParameter[] cloneParameters(VarMap varMap)
  {
    VarParameters pars = f.getParameters();
    VarParameter[] newPars = new VarParameter[pars.noParameters()];
    for (int i=0; i<pars.noParameters(); i++)
    {
      VarParameter p = pars.get(i);
      if (p.isRequired())
      {
        newPars[i] = p;
      }
      else
      {
        Expr newDefaultValue = p.getDefaultValue().clone(varMap);
        newPars[i] = new VarParameter(p.getBinding(), newDefaultValue);
      }
    }
    return newPars;
  }
  
  public void annotate()
  {
    VarParameters pars = f.getParameters();
    int p = pars.noParameters();
    if( p == 0 )
    {
      return;
    }
    ArrayList<Expr> uses = new ArrayList<Expr>();
    Expr body = f.body();
    for(int i = 0 ; i < p ; i++)
    {
      uses.clear();
      BindingExpr b = pars.get(i).getBinding();
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

  public Expr clone(VarMap varMap)
  {
    // clone parameters and body
    VarParameter[] newPars = cloneParameters(varMap);
    Expr newBody = f.body().clone(varMap);
    return new CaptureExpr(new JaqlFunction(new VarParameters(newPars), newBody));
  }
}
