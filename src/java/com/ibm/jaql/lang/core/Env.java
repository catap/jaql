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
import com.ibm.jaql.lang.core.Var.Scope;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.DoExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.function.FunctionCallExpr;
import com.ibm.jaql.lang.expr.top.EnvExpr;
import com.ibm.jaql.lang.expr.top.ExplainExpr;
import com.ibm.jaql.lang.expr.top.QueryExpr;
import com.ibm.jaql.lang.rewrite.VarTagger;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.lang.walk.PostOrderExprWalker;

/** An environment is namespace with a separate namespace for global variables. It is used
 * to store the compile-time environment. */
public class Env extends Namespace
{
  Namespace globals;   // holds global variables and imports
  Context context;     // compile-time context

  // -- construction ------------------------------------------------------------------------------
  
  public Env(Context context)
  {
    globals = new Namespace(null);
    this.context = context;
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
  
  /** Return the global variable called taggedName, or null if not found */
  public Var findGlobal(String taggedName)
  {
    return findVar(globals.variables, taggedName);
  }

  /**
   * Create a new immutable val variable with the specified name and put it into the global scope. 
   * The most recent definition of the global variable of the specified name is overwritten.
   */
  public Var scopeGlobalVal(String varName, Schema schema)
  {
    ensureNotFinal();
    Var var = findGlobal(varName);
    if ( var != null )
    {
      globals.unscope(var);
    }
    var = new Var(globals, varName, schema, Scope.GLOBAL, Var.State.FINAL);
    globals.scope(var);
    return var;
  }
  
  /**
   * Create a new immutable expr variable with the specified name and put it into the global scope. 
   * The most recent definition of the global variable of the specified name is shadowed.
   */
  public Var scopeGlobalExpr(String varName, Schema schema, Expr expr)
  {
    ensureNotFinal();
    Var var = findGlobal(varName);
    if ( var != null )
    {
      globals.unscope(var);
    }
    var = new Var(globals, varName, schema, Scope.GLOBAL, Var.State.FINAL);
    try
    {
      expr = expandMacros(expr);
    }
    catch(Exception ex)
    {
      JaqlUtil.rethrow(ex);
    }
    var.setExpr(expr);
    globals.scope(var);
    return var;
  }
  
  /**
   * If the varName is bound to a mutable global, return it.
   * Otherwise create a new mutable var variable with the specified name and puts it into the global scope, 
   * and the most recent definition of the immutable global variable of the specified name is shadowed.
   */
  public Var scopeGlobalMutable(String varName, Schema schema)
  {
    ensureNotFinal();
    Var var = findGlobal(varName);
    if ( var != null )
    {
      var = globals.inscope(varName);
      if( var.isMutable() )
      {
        return var;
      }
      globals.unscope(var);
    }
    var = new Var(globals, varName, schema, Scope.GLOBAL, Var.State.MUTABLE);
    globals.scope(var);
    return var;
  }
  
  /** 
   * Creates a new variable with the specified name and puts it into the global scope. 
   * The most recent definition of the global variable of the specified name is overwritten.
   * If varName contains a tag, this method will fail.
   */
  public Var setOrScopeMutableGlobal(String varName, JsonValue value)
  {
    Var var = scopeGlobalMutable(varName, SchemaFactory.anySchema());
    var.setValue(value);
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
  
  
  // -- compile-time evaluation -------------------------------------------------------------------
  
  /** Evaluate the expression using the environment's context. */
  public JsonValue eval(Expr e) throws Exception
  {
    VarTagger.tag(e);
    return e.eval(context);
  }
  
  // -- misc --------------------------------------------------------------------------------------

  /** Returns the global environment. */
  public Namespace globals()
  {
    return globals;
  }


  public Expr postParse(Expr root)
  {
    if( root == null )
    {
      return null;
    }

    if( !(root instanceof EnvExpr) )
    {
      root = new QueryExpr(this, root);
    }
    else if( root instanceof ExplainExpr )
    {
      if( !(root.child(0) instanceof EnvExpr) )
      {
        root.setChild(0, new QueryExpr(this,root.child(0)));
      }
    }
    
    try
    {
      PostOrderExprWalker walker = new PostOrderExprWalker();
      Expr expr;
      
      /***********
         // TODO: Should annotations be on Exprs or remain in the tree?
      // Pushdown annotations
      walker.reset(root);
      while( (expr = walker.next()) != null )
      {
        if( expr instanceof AnnotationExpr )
        {
          Expr anno = expr.child(0);
          if( !anno.isCompileTimeComputable().always() )
          {
            throw new IllegalArgumentException("annotations must be compile-time computable: "+expr);
          }
          JsonValue val = eval(anno);
          expr.child(1).addAnnotations((JsonRecord)val);
          expr.replaceInParent(expr.child(1));
        }
      }
      **********/

      // Import globals - note globals might still have macros in them.
      importGlobals(root);

      root = expandMacros(root);
      
      // Inline functions
      walker.reset(root);
      while( (expr = walker.next()) != null )
      {
        if( expr instanceof FunctionCallExpr )
        {
          FunctionCallExpr fc = (FunctionCallExpr)expr;
          Expr expr2 = fc.inlineIfPossible();
          if( expr2 != fc )
          {
            fc.replaceInParent(expr2);
          }
        }
      }
      
      return root;
    }
    catch( Exception e )
    {
      throw JaqlUtil.rethrow(e);
    }
  }
  
  /**
   * Expand any macros in the parse tree.
   * 
   * Assumes that macros do not have macro in their definition.
   */
  public Expr expandMacros(Expr root) throws Exception
  {
    if( root instanceof MacroExpr )
    {
      return ((MacroExpr) root).expand(this);
    }
    // Expand macros
    PostOrderExprWalker walker = new PostOrderExprWalker(root);
    Expr expr;
    while( (expr = walker.next()) != null )
    {
      if( expr instanceof MacroExpr )
      {
        Expr expr2 = ((MacroExpr)expr).expand(this);
        expr.replaceInParent( expr2 );
      }
    }
    return root;
  }
  
  /**
   * @param query
   * @return
   */
  public void importGlobals(Expr root)
  {
    assert root instanceof EnvExpr && root.numChildren() == 1;
    Expr top = root.child(0);
    if( root instanceof ExplainExpr )
    {
      assert top instanceof EnvExpr && top.numChildren() == 1;
      root = top;
      top = root.child(0);
    }
    HashMap<Var, Var> globalToLocal = new HashMap<Var, Var>();
    HashMap<Var, JsonValue> globalConst = new HashMap<Var, JsonValue>();
    ArrayList<Expr> bindings = new ArrayList<Expr>();
    VarMap varMap = new VarMap();
    importGlobalsAux(top, globalToLocal, globalConst, bindings, varMap);
    if (bindings.size() > 0)
    {
      bindings.add(top);
      top = new DoExpr(bindings);
      root.setChild(0, top);
    }
  }

  private void importGlobalsAux(
      Expr root,
      HashMap<Var, Var> globalToLocal,
      HashMap<Var, JsonValue> globalConst,
      ArrayList<Expr> bindings,
      VarMap varMap)
  {
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
          switch (var.type())
          {
            case EXPR: {
              Var localVar = globalToLocal.get(var);
              if (localVar == null)
              {
                localVar = makeVar(var.name());
                globalToLocal.put(var, localVar);
                varMap.clear();
                Expr localDef = var.expr().clone(varMap);
                Expr localRoot = new QueryExpr(this, localDef);
                importGlobalsAux(localDef, globalToLocal, globalConst, bindings, varMap);
                // Be sure to add the binding after recursion so that all dependent bindings
                // are bound before this one.
                localDef = localRoot.child(0);
                bindings.add(new BindingExpr(BindingExpr.Type.EQ, localVar, null, localDef));
              }
              ve.setVar(localVar);
              break;
            }
            case VALUE: {
              JsonValue val = globalConst.get(var);
              if( val == null )
              {
                val = JsonUtil.getCopyUnchecked(var.value(), null);
                globalConst.put(var, val);
              }
              ve.replaceInParent(new ConstExpr(val));
              break;
            }
            case UNDEFINED: {
              throw new IllegalStateException("use of undefined global variable: "+var.name());
            }
            default:
              throw new IllegalStateException("global variables have to have be of type value or expr");
          }
        }
      }
    }
  }
}
