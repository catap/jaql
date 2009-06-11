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
package com.ibm.jaql.lang.core;

import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.lang.walk.PostOrderExprWalker;

/** Stores the compile-time environment, i.e., a set of named {@link Var}s. Distinguishes 
 * between a local environment (represented by an instance of this class) and a special global 
 * environment (represented by another, static instance of this class). 
 * 
 */
public class Env
{
  private Env                  globalEnv;
  private HashMap<String, Var> nameMap = new HashMap<String, Var>();
  //  private HashMap<Var, Var> globalVars = new HashMap<Var, Var>(); // global vars imported into this local scope as this local var
  private int                  varId   = 0;

  /** Initializes an local environment. The global environment corresponding to this
   * environment is taken as the current value returned by {@link JaqlUtil#getSessionEnv()}. 
   * 
   */
  public Env()
  {
    globalEnv = JaqlUtil.getSessionEnv();
  }

  /**
   * 
   */
  public void reset()
  {
    nameMap.clear();
    //    globalVars.clear();
    varId = 0;
  }

  /**
   * Add a suffix to a variable to make it unique
   * 
   * @param var
   */
  public void makeUnique(Var var)
  {
    var.name = var.name + "__" + varId;
    varId++;
  }

//  public Var scope(String varName, Var.Type type)
//  {
//    Var var = new Var(varName, type, index);
//    index++;
//    var.varStack = nameMap.get(var.name);
//    nameMap.put(var.name, var);
//    return var;
//  }

  /**
   * Place a variable (back) in scope
   */
  public void scope(Var var)
  {
    var.varStack = nameMap.get(var.name);
    nameMap.put(var.name, var);
  }

  /** Creates a new variable with the specified name and puts it into the local scope.
   * Previous definitions of variables of the specified name are hidden but not overwritten.
   * 
   * @param varName
   * @return
   */
  public Var scope(String varName)
  {
    Var var = new Var(varName);
    scope(var);
    return var;
  }
  
  /** Returns the global environment. Must not be called from the instance of
   * Env that represents the global environment.
   * 
   * @return
   */
  public Env sessionEnv()
  {
    if (globalEnv == null)
    {
      throw new RuntimeException(
          "sessionEnv should only be called on a local scope");
    }
    return globalEnv;
  }

  /** Creates a new variable with the specified name and puts it into the global scope. 
   * The most recent definition of the variable of the specified name is overwritten. This 
   * method has to be called from the instance of Env that represents the global environment.
   * 
   * @param varName
   * @return
   */
  public Var scopeGlobal(String varName)
  {
    if (globalEnv != null)
    {
      throw new RuntimeException(
          "scopeGlobal should only be called on the global scope");
    }
    Var var = nameMap.get(varName);
    if (var != null)
    {
      unscope(var); // TODO: varName might still be on the globals scope... 
    }
    var = scope(varName);
    return var;
  }

  /** Removes the most recent definition of the specified variable from this scope. 
   * The most recent but one definition of the specified variable, if existent, 
   * becomes visible.
   * @param var
   */
  public void unscope(Var var)
  {
    nameMap.put(var.name, var.varStack);
    // TODO: we should be able to reduce the index and reuse space
    // index--;
  }

  /** Returns the variable of the specified name, searching in both the local and the
   * global scope (in this order).
   * 
   * @param varName
   * @return
   * @throws IndexOutOfBoundsException if varName is not defined or hidden
   */
  public Var inscope(String varName)
  {
    Var var = nameMap.get(varName);
    if (var == null)
    {
      if (globalEnv != null)
      {
        var = globalEnv.inscope(varName);
        //        Var globalVar = globalEnv.inscope(varName);
        //        var = globalVars.get(globalVar);
        //        if( var == null )
        //        {
        //          var = makeVar(varName); 
        //          globalVars.put(globalVar, var);
        //        }
      }
      else
      // this is the global env, so varName is not defined.
      {
        throw new IndexOutOfBoundsException("variable not defined: " + varName);
      }
    }
    if (var.hidden)
    {
      throw new IndexOutOfBoundsException("variable is hidden in this scope: "
          + varName);
    }
    return var;
  }

  /** Creates a new variable, scopes it, unscopes it, and returns it.
   * 
   * @param name
   * @return
   */
  public Var makeVar(String name) // FIXME: replace other scope()/unscope calls with this
  {
    assert name.charAt(0) == '$';
    Var var = scope(name);
    unscope(var);
    return var;
  }

  /**
   * @param root
   * @return
   */
  public Expr importGlobals(Expr root)
  {
    HashMap<Var, Var> globalToLocal = new HashMap<Var, Var>();
    ArrayList<Expr> bindings = new ArrayList<Expr>();
    VarMap varMap = new VarMap();
    PostOrderExprWalker walker = new PostOrderExprWalker(root);
    Expr expr;
    while ((expr = walker.next()) != null)
    {
      if (expr instanceof VarExpr)
      {
        VarExpr ve = (VarExpr) expr;
        Var var = ve.var();
        if (var.isGlobal())
        {
          Var localVar = globalToLocal.get(var);
          if (localVar == null)
          {
            localVar = makeVar(var.name);
            globalToLocal.put(var, localVar);
            Expr val;
// TODO: make global context and import from there.
//            if (var.value != null)
//            {
//              val = new ConstExpr(var.value);
//            }
//            else
            {
              varMap.clear();
              val = var.expr.clone(varMap);
              val = importGlobals(val);
            }
            bindings.add(new BindingExpr(BindingExpr.Type.EQ, localVar, null,
                val));
          }
          ve.setVar(localVar);
        }
      }
    }
    if (bindings.size() > 0)
    {
      bindings.add(root);
      root = new DoExpr(bindings);
    }
    return root;
  }

  private VarMap tempVarMap = new VarMap();
  public VarMap tempVarMap()
  {
    tempVarMap.clear();
    return tempVarMap;
  }

}
