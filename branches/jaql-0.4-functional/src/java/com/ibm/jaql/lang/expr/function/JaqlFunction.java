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
import com.ibm.jaql.lang.core.Env;
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
  public Expr inline(boolean eval)
  {
    if (eval)
    {
      return body;
    }
    else
    {
      // For functions with args, create a let to evaluate the args then call the body
      // TODO: Don't inline (potentially) recursive functions
      Expr[] doExprs = new Expr[noArgs+1];
      for (int i=0; i<noArgs; i++)
      {
        Var var = parameters.get(i).getVar();
        assert var.type() == Var.Type.EXPR; // all arguments are expressions at compile time
        doExprs[i] = new BindingExpr(BindingExpr.Type.EQ, var, null, var.expr());
      }
      doExprs[noArgs] = body;
      return new DoExpr(doExprs);
    }
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
      throw JaqlUtil.rethrow(e);
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
  
  // when body = (binding (, binding*) e), return e; else return body
  public JaqlFunction evalConstBindings()
  {
    JaqlFunction f = getCopy(null);
    f.body = evalConstBindings(f.body);
    return f;
  }
  
  private Expr evalConstBindings(Expr e)
  {
    if (e instanceof DoExpr)
    {
      Expr[] es = ((DoExpr)e).children();
      
      // test if right shape
      for (int i=0; i<es.length-1; i++)
      {
        if (!(es[i] instanceof BindingExpr))
        {
          return e;
        }
        BindingExpr be = (BindingExpr)es[i];
        if (be.type != BindingExpr.Type.EQ || be.eqExpr().isCompileTimeComputable().maybeNot())
        {
          return e;
        }
      }
      
      // go
      for (int i=0; i<es.length-1; i++)
      {
        BindingExpr be = (BindingExpr)es[i];
        try
        {
          be.var.setValue(be.eqExpr().eval(Env.getCompileTimeContext()));
          be.var.finalize();
        } catch (Exception e1)
        {
          throw JaqlUtil.rethrow(e1);
        }
      }
      return evalConstBindings(es[es.length-1]);
    }
    return e;
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
      if (par.isRequired())
      {
        newPars[i] = new VarParameter(newVar);
      }
      else
      {
        newPars[i] = new VarParameter(newVar, par.getDefaultValue());
      }
    }
    Expr newBody = body.clone(varMap);
    return new JaqlFunction(new VarParameters(newPars), newBody);
  }

  @Override
  public Function getImmutableCopy()
  {
    return getCopy(null);
  }

  @Override
  public void prepare(int noArgs)
  {
    this.noArgs = noArgs;
  }

  @Override
  protected void setArgument(int pos, JsonValue value)
  {
    parameters.get(pos).getVar().setValue(value);
  }

  @Override
  protected void setArgument(int pos, JsonIterator it)
  {
    parameters.get(pos).getVar().setIter(it);
  }

  @Override
  protected void setArgument(int pos, Expr expr)
  {
    parameters.get(pos).getVar().setExpr(expr);
  }

  @Override
  protected void setDefault(int pos)
  {
    setArgument(pos, parameters.defaultOf(pos));
  }
  
  public String formatError(String msg)
  {
    String result = "In call of Jaql function with signature fn(";  
    String sep="";
    for(int i=0; i<parameters.noParameters(); i++)
    {
      VarParameter p = parameters.get(i);
      result += sep + p.getName();
      if (p.isOptional())
      {
        result += "=??";
      }
      sep = ", ";
    }
    result += "): " + msg;
    return result;
  }
}
