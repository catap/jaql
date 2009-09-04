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
import com.ibm.jaql.util.Bool3;

public final class DefineJaqlFunctionExpr extends Expr
{
  private VarParameters parameters;
  
  protected static VarParameters makeParameters(Var[] params, Expr[] defaults)
  {
    assert params.length == defaults.length;
    VarParameter[] pars = new VarParameter[params.length];
    for(int i = 0 ; i < params.length ; i++)
    {
      if (defaults[i] == null)
      {
        pars[i] = new VarParameter(params[i]);
      }
      else
      {
        pars[i] = new VarParameter(params[i], defaults[i]);
      }
    }
    return new VarParameters(pars);
  }

  protected static VarParameters makeParameters(List<Var> params, List<Expr> defaults)
  {
    return makeParameters(params.toArray(new Var[params.size()]), 
        defaults.toArray(new Expr[defaults.size()]));
  }

  public DefineJaqlFunctionExpr(Var[] params, Expr body)
  {
    this(params, new Expr[params.length], body);
  }
  
  /**
   * @param params
   * @param body
   */
  public DefineJaqlFunctionExpr(Var[] params, Expr[] defaults, Expr body)
  {
    this(makeParameters(params, defaults), body);
  }

  /**
   * @param fnVar
   * @param params
   * @param body
   */
  public DefineJaqlFunctionExpr(List<Var> params, List<Expr> defaults, Expr body)
  {
    this(makeParameters(params, defaults), body);
  }
  
  public DefineJaqlFunctionExpr(VarParameters parameters, Expr body)
  {
    super(body);
    this.parameters = parameters;
    annotate();
  }
  
  public DefineJaqlFunctionExpr(List<Var> params, Expr body)
  {
    this(params, emptyList(params), body);
  }
  
  private static List<Expr> emptyList(List<Var> var) 
  {
    List<Expr> l = new ArrayList<Expr>(var.size());
    for (int i=0; i<var.size(); i++) l.add(null);
    return l;
  }
  
  private JaqlFunction getFunction()
  {
    return new JaqlFunction(parameters, body());
  }

  public Expr body()
  {
    return exprs[0];
  }
  
  public Bool3 getProperty(ExprProperty prop, boolean deep)
  {
    return super.getProperty(prop, false); // no deep property checks
  }
  
  @Override
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    if (getFunction().hasCaptures()) {
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
    JaqlFunction f = getFunction();
    exprText.print(f.getText());
    capturedVars.addAll(f.getCaptures());    
  }
  
  public HashSet<Var> getCapturedVars()
  {
    return getFunction().getCaptures();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JaqlFunction eval(Context context) throws Exception
  {
    JaqlFunction f = getFunction();
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
        Var newVar = varMap.get(v);
        es[i++] = new BindingExpr(BindingExpr.Type.EQ, newVar, null, new ConstExpr(val));
      }
      es[n] = newBody;
      
      // create function
      f = new JaqlFunction(new VarParameters(newPars), new DoExpr(es));
    }
    return f;
  }

  private VarParameter[] cloneParameters(VarMap varMap)
  {
    VarParameters pars = parameters;
    VarParameter[] newPars = new VarParameter[pars.numParameters()];
    for (int i=0; i<pars.numParameters(); i++)
    {
      VarParameter p = pars.get(i);
      if (p.isRequired())
      {
        newPars[i] = p;
      }
      else
      {
        Expr newDefaultValue = p.getDefaultValue().clone(varMap);
        newPars[i] = new VarParameter(p.getVar(), newDefaultValue);
      }
    }
    return newPars;
  }
  
  public void annotate()
  {
    VarParameters pars = parameters;
    int p = pars.numParameters();
    if( p == 0 )
    {
      return;
    }
    ArrayList<Expr> uses = new ArrayList<Expr>();
    Expr body = body();
    for(int i = 0 ; i < p ; i++)
    {
      uses.clear();
      Var var = pars.get(i).getVar();
      var.setUsage(Var.Usage.EVAL);
      body.getVarUses(var, uses);
      int n = uses.size();
      if( n == 0 )
      {
        var.setUsage(Var.Usage.UNUSED);
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
          var.setUsage(Var.Usage.STREAM);
        }
      }
    }
  }

  public Expr clone(VarMap varMap)
  {
    // clone parameters and body
    VarParameter[] newPars = cloneParameters(varMap);
    Expr newBody = body().clone(varMap);
    return new DefineJaqlFunctionExpr(new VarParameters(newPars), newBody);
  }
}
