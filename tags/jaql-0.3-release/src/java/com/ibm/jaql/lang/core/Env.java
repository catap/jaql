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
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.LetExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.lang.walk.PostOrderExprWalker;

/**
 * 
 */
public class Env
{
  private Env                  globalEnv;
  private HashMap<String, Var> nameMap = new HashMap<String, Var>();
  //  private HashMap<Var, Var> globalVars = new HashMap<Var, Var>(); // global vars imported into this local scope as this local var
  private int                  index   = 0;
  private int                  varId   = 0;

  /**
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
    index = 0;
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

  /**
   * @param varName
   * @return
   */
  public Var scope(String varName)
  {
    Var var = new Var(varName, index);
    index++;
    var.varStack = nameMap.get(var.name);
    nameMap.put(var.name, var);
    return var;
  }

  /**
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

  /**
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
      unscope(var);
    }
    var = scope(varName);
    return var;
  }

  /**
   * @param var
   */
  public void unscope(Var var)
  {
    nameMap.put(var.name, var.varStack);
    // TODO: we should be able to reduce the index and reuse space
    // index--;
  }

  /**
   * @param varName
   * @return
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

  /**
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
    ArrayList<BindingExpr> bindings = new ArrayList<BindingExpr>();
    VarMap varMap = new VarMap(this);
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
            if (var.value != null)
            {
              val = new ConstExpr(var.value);
            }
            else
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
      root = new LetExpr(bindings, root);
    }
    return root;
  }

}
