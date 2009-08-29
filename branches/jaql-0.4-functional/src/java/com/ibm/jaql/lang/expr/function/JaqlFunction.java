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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashSet;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
public class JaqlFunction extends Function
{
  private int noArgs = -1;
  private int noExprArgs = -1;
  private VarParameters parameters;
  private Expr body;
  
  /**
   * 
   */
  public JaqlFunction(VarParameters parameters, Expr body)
  {
    this.parameters = parameters;
    this.body = body;
  }

  
  @Override
  public Expr inline()
  {
    return inline(false);
  }
  
  public JsonValue eval(Context context) throws Exception
  {
    Expr f = inline(true); 
    return f.eval(context);
  }

  public JsonIterator iter(Context context) throws Exception
  {
    Expr f = inline(true);  
    return f.iter(context);
  }
  
  protected Expr inline(boolean eval)
  {
    assert noExprArgs >= 0;
    assert eval || noExprArgs == noArgs;
    
    // For functions with args, create a let to evaluate the args then call the body
    // TODO: Don't inline (potentially) recursive functions
    Expr[] doExprs = new Expr[noExprArgs+1];
    int p = 0;
    for (int i=0; i<noArgs; i++)
    {
      BindingExpr binding = parameters.get(i).getBinding();
      if (binding.numChildren() == 1) // all expressions!
      {
        doExprs[p] = binding;
        ++p;
      }
    }
    assert p == noExprArgs;
    doExprs[noArgs] = body;
    return new DoExpr(doExprs);
  }
  
  @Override
  public VarParameters getParameters()
  {
    return parameters;
  }
  
  public HashSet<Var> getCapturedVars()
  {
    // TODO: make more efficient
    HashSet<Var> capturedVars = new HashSet<Var>();
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream exprText = new PrintStream(outStream);
    try
    {
      body.decompile(exprText, capturedVars);
      for (int i=0; i<parameters.noParameters(); i++)
      {
        VarParameter par = parameters.get(i);
        capturedVars.remove(par.getVar());
        if (par.isOptional())
        {
          par.getDefaultValue().decompile(exprText, capturedVars);
        }
      }
      return capturedVars;
    }
    catch (Exception e)
    {
      JaqlUtil.rethrow(e);
      return null;
    }
  }
  
  public boolean hasCaptures()
  {
    return !getCapturedVars().isEmpty();
  }
  
  public Expr body()
  {
    return body;
  }
  
  @Override
  public JaqlFunction getCopy(JsonValue target)
  {
    VarMap varMap = new VarMap();
    int n = parameters.noParameters();
    VarParameter[] newPars = new VarParameter[n];
    for (int i=0; i<n; i++)
    {
      VarParameter par = parameters.get(i); 
      Var oldVar = par.getVar();
      Var newVar = new Var(oldVar.name(), oldVar.getSchema());
      varMap.put(oldVar, newVar);
      BindingExpr binding = new BindingExpr(BindingExpr.Type.EQ, newVar, null, Expr.NO_EXPRS);
      if (par.isRequired())
      {
        newPars[i] = new VarParameter(binding);
      }
      else
      {
        newPars[i] = new VarParameter(binding, par.getDefaultValue());
      }
    }
    Expr newBody = body.clone(varMap);
    return new JaqlFunction(new VarParameters(newPars), newBody);
  }

  @Override
  public Function getImmutableCopy() throws Exception
  {
    return getCopy(null);
  }

  @Override
  public void prepare(int noArgs)
  {
    this.noArgs = noArgs;
    this.noExprArgs = 0;
  }

  @Override
  protected void setArgument(int pos, JsonValue value)
  {
    BindingExpr binding = parameters.get(pos).getBinding();
    while (binding.numChildren() > 1)
    {
      binding.removeChild(0);
    }
    binding.var.setValue(value);
  }

  @Override
  protected void setArgument(int pos, JsonIterator it)
  {
    BindingExpr binding = parameters.get(pos).getBinding();
    while (binding.numChildren() > 1)
    {
      binding.removeChild(0);
    }
    binding.var.setIter(it);
  }

  @Override
  protected void setArgument(int pos, Expr expr)
  {
    ++this.noExprArgs;
    BindingExpr binding = parameters.get(pos).getBinding();
    while (binding.numChildren() > 1)
    {
      binding.removeChild(0);
    }
    binding.addChild(expr);
  }

  @Override
  protected void setDefault(int pos)
  {
    setArgument(pos, parameters.defaultOf(pos));
  }
}
