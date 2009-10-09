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
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.walk.PostOrderExprWalker;

/** An environment is namespace with a separate namespace for global variables. It is used
 * to store the compile-time environment. */
public class Env extends Namespace
{
  Namespace   globals;   // holds global variables and imports
  

  // -- construction ------------------------------------------------------------------------------
  
  public Env()
  {
    globals = new Namespace(null);   
  }
  
  
  // -- imports -----------------------------------------------------------------------------------
  
  @Override
  public void importNamespace(Namespace namespace) {
    globals.importNamespace(namespace);
  }
  
  @Override
  public void importAllFrom(Namespace namespace) {
    globals.importAllFrom(namespace);
  }
  
  @Override
  public void importFrom(Namespace namespace, ArrayList<String> varNames) {
    globals.importFrom(namespace, varNames);
  }


  // -- scoping -----------------------------------------------------------------------------------
  
  @Override
  public Var inscope(String taggedName)
  {
    Var var = findVar(variables, taggedName);
    if (var == null)
    {
      return inscopeGlobal(taggedName);
    }
    if (var.isHidden()) 
    {
      throw new IndexOutOfBoundsException("variable is hidden in this scope: " + var.taggedName());
    }
    return var;
  }
  
  @Override
  public Var inscopeLocal(String taggedName)
  {
    Var var = findVar(variables, taggedName);
    if (var == null)
    {
      return globals().inscopeLocal(taggedName);
    }
    if (var.isHidden()) 
    {
      throw new IndexOutOfBoundsException("variable is hidden in this scope: " + var.taggedName());
    }
    return var;
  }
  
  public boolean isDefinedLocal(String taggedName)
  {
    try
    {
      inscopeLocal(taggedName);
      return true;
    }
    catch (Exception e)
    {
      return false;
    }
  }
  
  @Override
  public Var inscopeImport(String namespace, String varName)
  {
    return globals.inscopeImport(namespace, varName);
  }

  public Var inscopeGlobal(String varName)
  {
    return globals.inscope(varName);
  }
  
  /** Removes all local variables from this environment. */
  public void reset()
  {
    ensureNotFinal();
    variables.clear();
  }

  /** 
   * Creates a new variable with the specified name and puts it into the global scope. 
   * The most recent definition of the global variable of the specified name is overwritten.
   */
  public Var scopeGlobal(String varName, Schema schema)
  {
    ensureNotFinal();
    if (globals.variables.containsKey(varName))
    {
      globals.unscope(globals.inscope(varName));
    }
    Var var = new Var(varName, schema, true);
    globals.scope(var);
    return var;
  }
  
  /** 
   * Creates a new variable with the specified name and puts it into the global scope. 
   * The most recent definition of the global variable of the specified name is overwritten.
   */
  public Var scopeGlobal(String varName) 
  {
    return scopeGlobal(varName, SchemaFactory.anySchema());
  }
  
  /** 
   * Creates a new variable with the specified name and puts it into the global scope. 
   * The most recent definition of the global variable of the specified name is overwritten.
   * If varName contains a tag, this method will fail.
   */
  public Var scopeGlobal(String varName, JsonValue value)
  {
    ensureNotFinal();
    if (globals.variables.containsKey(varName))
    {
      globals.unscope(globals.inscope(varName));
    }
    Var var = new Var(varName, SchemaFactory.schemaOf(value), true);
    var.setValue(value);
    scope(var);
    var.finalize();
    return var;
  }
  
  
  // -- variables ---------------------------------------------------------------------------------
  
  /** Creates a new variable, scopes it, unscopes it, and returns it. */
  public Var makeVar(String name) 
  {
    return makeVar(name, SchemaFactory.anySchema());
  }

  /** Creates a new variable, scopes it, unscopes it, and returns it. */
  public Var makeVar(String name, Schema schema) 
  {
    assert schema != null;
    Var var = scope(name, schema);
    unscope(var);
    return var;
  }
  
  
  // -- misc --------------------------------------------------------------------------------------

  /** Returns the global environment. */
  public Namespace globals()
  {
    return globals;
  }

  /** Preliminary method. Used to obtain a context at compile time. */
  // FIXME: We require a context here! The context should be passed in to this function.
  // The context cannot be reset after this call because parts of the result may be in the context
  // temp space.  The temp space needs to live as long as the expression tree lives.  (This is true of
  // all constants in the tree.)  As a temporary HACK, we are using a null context.  If there is a expr that
  // reports isConst and requires the context, we will get a null pointer exception, which is a bug on our part.
  // Either we need to pass a context around during parsing, or we need to defer this evaluation to after parsing.

  // FIXME: we have to create a single point for all compile-time evals and tag variables over there
  
  private static Context compileTimeContext = new Context();
  public static Context getCompileTimeContext()
  {
    // FIXME: this is just a quick hack to support compile time compilation
    return compileTimeContext;
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
        if (var.isGlobal() && (var.getNamespace() != SystemNamespace.getInstance()))
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
}
