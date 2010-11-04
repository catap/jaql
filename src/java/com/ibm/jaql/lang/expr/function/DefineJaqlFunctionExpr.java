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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.FastPrinter;

/** Definition of a Jaql function. 
 * 
 * The children are of form <a>* <b>* <c>, where
 * <a> BindingExpr without a child (required parameter), 
 * <b> a BindingExpr with default value as child (optional parameter),
 * <c> function body.
 */
public final class DefineJaqlFunctionExpr extends Expr
{
  protected Schema schema;

  // -- construction ------------------------------------------------------------------------------
  
  /** Construct function definition without default values */
  public DefineJaqlFunctionExpr(Var[] params, Expr body)
  {
    this(params, new Expr[params.length], body);
  }

  /** Construct function definition without default values */
  public DefineJaqlFunctionExpr(List<Var> params, Expr body)
  {
    this(params, emptyList(params.size()), body);
  }

  public DefineJaqlFunctionExpr(Var[] params, Expr[] defaults, Schema schema, Expr body)
  {
    super(makeParameters(params, defaults, body));
    this.schema = schema;
    getFunction(); // checks that arguments are valid
  }
  
  /** Construct function definition with given default values */
  public DefineJaqlFunctionExpr(Var[] params, Expr[] defaults, Expr body)
  {
    this(params, defaults, SchemaFactory.anySchema(), body);
  }

  /** Construct function definition with given default values */
  public DefineJaqlFunctionExpr(List<Var> params, List<Expr> defaults, Expr body)
  {
    this(params, defaults, SchemaFactory.anySchema(), body);
  }
  
  /** Construct function definition with given default values */
  public DefineJaqlFunctionExpr(List<Var> params, List<Expr> defaults, Schema schema, Expr body)
  {
    super(makeParameters(params, defaults, body));
    this.schema = schema;
    getFunction(); // checks that arguments are valid
  }

  public DefineJaqlFunctionExpr(VarParameters parameters, Expr body)
  {
    this(parameters, SchemaFactory.anySchema(), body);
  }

  /** Construct function definition from given parameters */
  public DefineJaqlFunctionExpr(VarParameters parameters, Schema schema, Expr body)
  {
    super(makeParameters(parameters, body));
    this.schema = schema;
    getFunction(); // checks that arguments are valid
  }
  
  /** Construct the children of a DefineJaqlFunctionExpr with the given parameters and body. */
  protected static Expr[] makeParameters(Var[] params, Expr[] defaults, Expr body)
  {
    assert params.length == defaults.length;
    Expr[] exprs = new Expr[params.length+1];
    for(int i = 0 ; i < params.length ; i++)
    {
      if (defaults[i] == null)
      {
        exprs[i] = new BindingExpr(BindingExpr.Type.EQ, params[i], null, Expr.NO_EXPRS);
      }
      else
      {
        exprs[i] = new BindingExpr(BindingExpr.Type.EQ, params[i], null, defaults[i]);
      }
    }
    exprs[exprs.length-1] = body;
    return exprs;
  }

  /** @see #makeParameters(Var[], Expr[], Expr) */
  protected static Expr[] makeParameters(List<Var> params, List<Expr> defaults, Expr body)
  {
    return makeParameters(params.toArray(new Var[params.size()]), 
        defaults.toArray(new Expr[defaults.size()]), body);
  }

  /** @see #makeParameters(Var[], Expr[], Expr) */
  protected static Expr[] makeParameters(VarParameters pars, Expr body)
  {
    int n = pars.numParameters();
    Var[] params = new Var[n];
    Expr[] defaults = new Expr[n];
    for (int i=0; i<n; i++)
    {
      VarParameter p = pars.get(i);
      params[i] = p.getVar();
      defaults[i] = p.isRequired() ? null : p.getDefaultValue();
    }
    return makeParameters(params, defaults, body);
  }
  
  /** Construct an empty list of the specified size */
  private static List<Expr> emptyList(int size) 
  {
    List<Expr> l = new ArrayList<Expr>(size);
    for (int i=0; i<size; i++) l.add(null);
    return l;
  }
  
  
  // -- getters -----------------------------------------------------------------------------------
  
  /** Returns the function body */
  public Expr body()
  {
    return exprs[exprs.length-1];
  }
  
  public void setBody(Expr newBody)
  {
    this.setChild(exprs.length-1, newBody);
  }
  
  /** Returns the number of parameters of this function */
  public int numParams()
  {
    return exprs.length-1;
  }
  
  /** Returns the variable used for parameter <tt>i</tt> */
  public Var varOf(int i)
  {
    BindingExpr e = (BindingExpr)exprs[i];
    return e.var;
  }
  
  /** Returns the default value of parameter <tt>i</tt> or <code>null</code> if the parameter
   * does not have a default value */
  public Expr defaultOf(int i)
  {
    BindingExpr e = (BindingExpr)exprs[i];
    if (e.numChildren() == 0)
    {
      return null;
    }
    else
    {
      return ((BindingExpr)e).eqExpr();
    }
  }
  
  // package private; might return a function with free variables, which is not a valid literal
  JaqlFunction getFunction()
  {
    int n = numParams();
    VarParameter[] pars = new VarParameter[n];
    for (int i=0; i<n; i++)
    {
      Var var = varOf(i);
      Expr defaultValue = defaultOf(i);
      if (defaultValue == null)
      {
        pars[i] = new VarParameter(var);
      }
      else
      {
        pars[i] = new VarParameter(var, defaultValue);
      }
    }
    return new JaqlFunction(new VarParameters(pars), body());
  }
  

  // -- Expr --------------------------------------------------------------------------------------

  @Override
  public Bool3 getProperty(ExprProperty prop, boolean deep)
  {
 // no deep property checks: it's just a function definition
    return super.getProperty(prop, false);  
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
  
  @Override
  public Schema getSchema()
  {
    return schema;
  }
  
  @Override
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    JaqlFunction f = getFunction();    
    exprText.print(f.getText());
    capturedVars.addAll(f.getCaptures());    
  }
  
  @Override
  public HashSet<Var> getCapturedVars()
  {
    return getFunction().getCaptures();
  }

  @Override
  public Expr clone(VarMap varMap)
  {
    // clone parameters and body, using fresh variables for the parameters 
    VarParameter[] newPars = cloneParameters(varMap);
    Expr newBody = body().clone(varMap);
    return new DefineJaqlFunctionExpr(new VarParameters(newPars), schema, newBody);
  }
  
  /** Clone the parameters and their default values. */
  private VarParameter[] cloneParameters(VarMap varMap)
  {
    VarParameter[] newPars = new VarParameter[numParams()];
    for (int i=0; i<numParams(); i++)
    {
      Var var = varOf(i);
      Expr defaultValue = defaultOf(i);
      if (defaultValue == null)
      {
        newPars[i] = new VarParameter(var);
      }
      else
      {
        Expr newDefaultValue = defaultValue.clone(varMap);
        newPars[i] = new VarParameter(var, newDefaultValue);
      }
    }
    return newPars;
  }
  
  // -- evaluation --------------------------------------------------------------------------------
  
  
  @Override
  public JaqlFunction eval(Context context) throws Exception
  {
    annotate();
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
      for (Var oldVar: capturedVars)
      {
        Var newVar = new Var(oldVar.taggedName(), oldVar.getSchema());
        varMap.put(oldVar, newVar);
      }
      
      // clone parameters and body
      VarParameter[] newPars = cloneParameters(varMap);
      Expr newBody = f.body().clone(varMap);
      
      // capture variables
      Map<Var, JsonValue> localBindings = new HashMap<Var, JsonValue>();
      for( Var v: capturedVars )
      {
        JsonValue val = JsonUtil.getCopy(v.getValue(context), null);
        Var newVar = varMap.get(v);
        localBindings.put(newVar, val);
      }
      
      // create function      
      f = new JaqlFunction(localBindings, new VarParameters(newPars), newBody);
      f.tagVars();
    }
    
    return f;
  }

  /** Annotate the parameter variables, see {@link Var.Usage}. */ 
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
      Var var = varOf(i);
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
}
