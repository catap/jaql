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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.lang.expr.path.PathFieldValue;
import com.ibm.jaql.lang.expr.top.EnvExpr;
import com.ibm.jaql.util.FastPrinter;

/** A variable.
 * 
 */
public class VarExpr extends Expr
{
  Var var;
  
  public VarExpr(Var var)
  {
    super(NO_EXPRS);
    this.var=var;
  }
  
  
//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#isConst()
//   */
//  @Override
//  public boolean isConst()
//  {
//    // TODO: do this?
//    //    if( var.value != null )
//    //    {
//    //      return true;
//    //    }
//    return false;
//  }
  
  /**
   * Return the mapping table.
   */
  @Override
  public MappingTable getMappingTable()
  {
	  MappingTable mt = new MappingTable();
	  VarExpr ve = new VarExpr(new Var(MappingTable.DEFAULT_PIPE_VAR));  
	  mt.add(ve, this, true);
	  return mt;
  }
  

  @Override
  public Schema getSchema()
  {
    return var.getSchema();
  }

  //  public VarExpr(Env env, String varName)
  //  {
  //    this(env.inscope(varName));
  //  }
  //
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    String name;
    EnvExpr envExpr = this.getEnvExpr();
    if( envExpr == null )
    {
      // We have an incomplete tree -- can't print module alias
      // This should only happen in the debugger...
      name = var.taggedName();
      if( var.isGlobal() )
      {
        name = "unknown::" + name;
      }
    }
    else
    {
      name = var.qualfiedName( envExpr.getEnv().globals );
    }
    exprText.print(name);
    capturedVars.add(var);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  public VarExpr clone(VarMap varMap)
  {
    Var v = varMap.get(var);
    return new VarExpr(v);
  }

  
  public Map<ExprProperty, Boolean> getProperties() 
  {
    if (var.isFinal())
    {
      switch (var.type())
      {
      case VALUE:
        Map<ExprProperty, Boolean> result = ExprProperty.createUnsafeDefaults();
        result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
        return result;
      case EXPR:
        return var.expr().getProperties();
      default:
        throw new IllegalStateException("final variable not of type VALUE or EXRP");
      }
    }
    return super.getProperties();
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception
  {
    JsonValue result = var.getValue(context); 
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(Context context) throws Exception
  {
    return var.iter(context);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#replaceVar(com.ibm.jaql.lang.core.Var,
   *      com.ibm.jaql.lang.core.Var)
   */
  public void replaceVar(Var oldVar, Var newVar)
  {
    if (oldVar == var)
    {
      var = newVar;
      subtreeModified();
    }
  }
  
  /**
   * Replace all uses of $oldVar with $recVar.fieldName
   * 
   * @param oldVar
   * @param recVar
   * @param fieldName
   * @return
   */
  public Expr replaceVar(Var oldVar, Var recVar, String fieldName)
  {
    if (oldVar == var)
    {
      Expr proj = PathFieldValue.byName(recVar, fieldName);
      this.replaceInParent(proj);
      return proj;
    }
    return this;
  }

  /**
   * @return
   */
  public final Var var()
  {
    return var;
  }

  /**
   * @param var
   */
  public void setVar(Var var)
  {
    this.var = var;
  }
  
  @Override
  public void getVarUses(Var var, ArrayList<Expr> uses)
  {
    if( this.var == var )
    {
      uses.add(this);
    }
  }

  /**
   * @return The ancestor Expr that defines the var used in this VarExpr.
   */
  public BindingExpr findVarDef()
  {
    return parent.findVarDef(var);
  }
}
