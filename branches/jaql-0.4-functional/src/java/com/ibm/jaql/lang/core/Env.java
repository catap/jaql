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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
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
  protected NamespaceEnv         namespaceEnv = null;
  protected HashMap<String, Var> nameMap = new HashMap<String, Var>();
  private int                  varId   = 0;

  /** Initializes an local environment. The global environment corresponding to this
   * environment is taken as the current value returned by {@link JaqlUtil#getSessionEnv()}. 
   * 
   */
  public Env()
  {
  	namespaceEnv = new NamespaceEnv();
  }
  
  public Env(boolean t) {
  	//Dummy placeholder to avoid infite recursion when creating
  	//the namespace environment
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
    var.setName(var.name() + "__" + varId);
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
    var.varStack = nameMap.get(var.name());
    nameMap.put(var.name(), var);
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

  /** Creates a new variable with the specified name and schema and puts it into the local scope.
   * Previous definitions of variables of the specified name are hidden but not overwritten.
   * 
   * @param varName
   * @return
   */
  public Var scope(String varName, Schema varSchema)
  {
    Var var = new Var(varName, varSchema);
    scope(var);
    return var;
  }
  
    /** Returns the global environment. Must not be called from the instance of
   * Env that represents the global environment.
   * 
   * @return
   */
  public NamespaceEnv namespaceEnv()
  {
    if (namespaceEnv == null)
    {
      throw new RuntimeException(
          "namespaceEnv should only be called on a local scope");
    }
    return namespaceEnv;
  }

  
  /** Removes the most recent definition of the specified variable from this scope. 
   * The most recent but one definition of the specified variable, if existent, 
   * becomes visible.
   * @param var
   */
  public void unscope(Var var)
  {
    nameMap.put(var.name(), var.varStack);
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
      if (namespaceEnv != null)
      {
        var = namespaceEnv.inscope(varName);
      }
      else
      // this is the global env, so varName is not defined.
      {
    	throw new IndexOutOfBoundsException("variable not defined: " + varName);
      }
    }
    if (var.isHidden())
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
    return makeVar(name, SchemaFactory.anySchema());
  }

  public Var makeVar(String name, Schema schema) // FIXME: replace other scope()/unscope calls with this
  {
    assert schema != null;
    Var var = scope(name, schema);
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
            localVar = makeVar(var.name());
            globalToLocal.put(var, localVar);
            Expr e;
            switch (var.type())
            {
            case EXPR:
           // TODO: make global context and import from there.
//            if (var.value != null)
//            {
//              val = new ConstExpr(var.value);
//            }
//            else
              {
                varMap.clear();
                e = var.expr().clone(varMap);
                e = importGlobals(e);
              }
              break;
            case VALUE:
              e = new ConstExpr(JsonUtil.getCopyUnchecked(var.value(), null));
              break;
            default:
              throw new IllegalStateException("global variables have to have be of type value or expr");
            }
            bindings.add(new BindingExpr(BindingExpr.Type.EQ, localVar, null, e));
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

  /** Preliminary method. Used to obtain a context at compile time. */
  // FIXME: We require a context here! The context should be passed in to this function.
  // The context cannot be reset after this call because parts of the result may be in the context
  // temp space.  The temp space needs to live as long as the expression tree lives.  (This is true of
  // all constants in the tree.)  As a temporary HACK, we are using a null context.  If there is a expr that
  // reports isConst and requires the context, we will get a null pointer exception, which is a bug on our part.
  // Either we need to pass a context around during parsing, or we need to defer this evaluation to after parsing.
  private static Context compileTimeContext = new Context();
  public static Context getCompileTimeContext()
  {
    // FIXME: this is just a quick hack to support compile time compilation
    return compileTimeContext;
  }
}
