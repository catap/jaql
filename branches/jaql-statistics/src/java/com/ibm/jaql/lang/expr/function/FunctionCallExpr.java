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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.util.JaqlUtil;

// TODO: optimize the case when the fn is known to have a IterExpr body
/**
 * An expression representing a function call.
 */
public class FunctionCallExpr extends Expr
{
  // [ fn, ( nameExpr , valueExpr )* ]
  // nameExpr can be instance of NoNameExpr, meaning that the argument is positional
  
  private static final Map<JsonString, Expr> NO_NAMED_ARGS 
    = Collections.unmodifiableMap(new HashMap<JsonString, Expr>(0));
  
  
  // -- construction ------------------------------------------------------------------------------
  
  /** exprs[0](exprs[1:*]) */
  public FunctionCallExpr(Expr[] exprs)
  {
    super(exprs);
  }
  
  public FunctionCallExpr(Expr fn, List<Expr> positionalArgs, Map<JsonString, Expr> namedArgs)
  {
    this(makeExprs(fn, positionalArgs, namedArgs));
  }

  public FunctionCallExpr(Expr fn, Expr ... positionalArgs)
  {
    this(makeExprs(fn, Arrays.asList(positionalArgs), NO_NAMED_ARGS));
  }

  /** Construct the children of a FunctionCallExpr with the given function and arguments. */ 
  public static Expr[] makeExprs(Expr fn, List<Expr> args, Map<JsonString, Expr> namedArgs)
  {
    int np = args.size(); 
    int nn = namedArgs.size();
    int n = np+nn;
    Expr[] exprs = new Expr[2*n + 1];
    exprs[0] = fn;

    for (int i = 0; i < np; i++)
    {
      exprs[2*i+1] = new NoNameExpr();
      exprs[2*i+1+1] = args.get(i);
    }
    Iterator<Entry<JsonString, Expr>> it = namedArgs.entrySet().iterator();
    for (int i = 0; it.hasNext(); i++)
    {
      Entry<JsonString, Expr> e = it.next();
      exprs[2*(i+np)+1] = new ConstExpr(e.getKey());
      exprs[2*(i+np)+1+1] = e.getValue();
    }
    return exprs;
  }


  // -- getters -----------------------------------------------------------------------------------
  
  /** Returns the expression for the function being called */
  public final Expr fnExpr()
  {
    return exprs[0];
  }

  /** Returns the function if compile-time computable; otherwise returns <code>null</code>. */
  protected Function getFunction()
  {
    if (fnExpr().isCompileTimeComputable().always())
    {
      try
      {
        return (Function)fnExpr().compileTimeEval();
      } catch (Exception e)
      {
        return null;
      }
    }
    return null;
  }
  
  /** Returns number of arguments provided in the call */
  public final int numArgs()
  {
    return (exprs.length - 1)/2;
  }
  
  /** Checks whether the i-th argument has a name (true) or is positional (false) */
  public boolean hasName(int i)
  {
    Expr e = exprs[2*i+1];
    if (e instanceof NoNameExpr) return false;
    try
    {
      if (e.isCompileTimeComputable().always() && e.compileTimeEval()==null)
      {
        return false;
      }
    } catch (Exception e1)
    {
      JaqlUtil.rethrow(e1);
    }
    return true;
  }
  
  /** Returns the name of the i-th argument. Should only be called after <code>hasName()</code>. */
  public Expr name(int i)
  {
    assert hasName(i);
    return exprs[2*i+1];
  }
  
  /** Returns the value of the i-th argument */
  public Expr arg(int i)
  {
    return exprs[2*i+1+1];
  }

  
  // -- Expr --------------------------------------------------------------------------------------

  @Override
  public Map<ExprProperty, Boolean> getProperties()
  {
    Function f = getFunction();
    if (f instanceof BuiltInFunction || f instanceof JaqlFunction)
    {
      f = f.getCopy(null);
      f.setArguments(exprs, 1, exprs.length - 1, true);
      Expr e = f.inline(true);
      return e.getProperties();
    }
    return ExprProperty.createSafeDefaults();
  }
  
  @Override
  public Schema getSchema()
  {
    Function f = getFunction();
    if (f instanceof BuiltInFunction || f instanceof JaqlFunction)
    {
      f = f.getCopy(null);
      f.setArguments(exprs, 1, exprs.length - 1, true);
      Expr e = f.inline(true);
      return e.getSchema();
    }
    return SchemaFactory.anySchema();
  }
  
  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("(");
    fnExpr().decompile(exprText, capturedVars);
    exprText.print(")");
    exprText.print("(");
    char sep = ' ';
    for (int i = 0; i < numArgs(); i++)
    {
      exprText.print(sep);
      if (hasName(i))
      {
        exprText.print((JsonString)((ConstExpr)name(i)).value); // TODO: change this if more general expr are allowed
        exprText.print("=");
      }
      exprText.print("(");
      arg(i).decompile(exprText, capturedVars);
      exprText.print(")");
      sep = ',';
    }
    exprText.print(")");
  }
  

  // -- inlining ----------------------------------------------------------------------------------

  /** Inline this function. Throws an exception when the function expression is not compile-time
   * computable; see {@link #inlineIfPossible()} */
  public Expr inline() 
  {
    // inline Jaql function definitions
    if (exprs[0] instanceof DefineJaqlFunctionExpr)
    {
      Function f = ((DefineJaqlFunctionExpr)exprs[0]).getFunction(); // may have free variables
      f = f.getCopy(null);
      f.setArguments(exprs, 1, exprs.length - 1, true);
      return f.inline(false);
    }
    
    // inline compile-time computable functions
    if (exprs[0].isCompileTimeComputable().maybeNot()) 
    {
      throw new IllegalStateException("only functions known at compile time can be inlined");
    }
    try
    {
      Function f = (Function)exprs[0].compileTimeEval();
      if( f == null )
      {
        return new ConstExpr(null);
      }
      f = f.getCopy(null);
      f.setArguments(exprs, 1, exprs.length - 1, true);
      return f.inline(false);      
    }
    catch (Exception e)
    {
      throw JaqlUtil.rethrow(e);
    }
  }
  
  /** Inline this function, if possible. Otherwise, return this function call expression */
  public Expr inlineIfPossible()
  {
    return inlineIfPossible(this);
  }
  
  /** If e is a function call, inline it if possible at compile time. Otherwise, return e */
  public static Expr inlineIfPossible(Expr e)
  {
    if (e instanceof FunctionCallExpr)
    {
      FunctionCallExpr fe = (FunctionCallExpr)e;
      try
      {
        return fe.inline();
      }
      catch (Exception ex)
      {
        return e;
      }
    }
    return e;
  }
  

  // -- evaluation --------------------------------------------------------------------------------
  
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    Function fn = (Function)exprs[0].eval(context);
    if( fn == null )
    {
      return null;
    }
    fn = context.getCallable(this, fn);
    fn.setArguments(exprs, 1, exprs.length - 1, true);
    return fn.eval(context);
//    else if( fnVal instanceof JString )
//    {
//      //PythonInterpreter interp = new PythonInterpreter();
//      CompilerFlags cflags = new CompilerFlags();
//      String kind = "eval"; // eval, exec, single
//      String filename = "<jaql>";
//
//      String fnName = fnVal.toString().intern();
//      PyModule module = context.getPyModule();
//      PyObject locals = module.__dict__;
//      PyObject fnObj = locals.__finditem__(fnName);
//      if( fnObj == null )
//      {
//        // __builtin__.getattr(__builtin__.globals(), new PyString(fnName));
//        // TODO: there has to be a better way to find functions...
//        PyObject code = Py.compile_flags(fnName, filename, kind, cflags);
//        fnObj = Py.runCode((PyCode)code, locals, locals);
//        if( fnObj == null )
//        { 
//          throw new RuntimeException("function not found: "+fnName);
//        }
//      }
//      if( !( fnObj instanceof PyFunction ) &&
//          !( fnObj instanceof PyBuiltinFunction ) &&
//          !( fnObj instanceof PyType ) ) // generator
//      {
//        throw new RuntimeException("not a function: "+fnName);
//      }
//      int n = exprs.length-1;
//      PyObject[] pyArgs = new PyObject[n];
//
//      for (int i = 0 ; i < n ; i++)
//      {
//        Item item = exprs[i+1].eval(context);
//        String json = item.toJSON();
//        PyObject code = Py.compile_flags(json, filename, kind, cflags);
//        pyArgs[i] = Py.runCode((PyCode)code, locals, locals);
//        // pyArgs[i] = interp.eval(json); // TODO: make a faster conversion between python and jaql; and do Decimals; null<->None; sequence<->array; long suffix
//      }
//      // interp.cleanup();
//
//      PyObject result;
//      if( fnObj instanceof PyFunction )
//      {
//        PyFunction fn = (PyFunction)fnObj;
//        result = fn.__call__(pyArgs);
//      }
//      else if( fnObj instanceof PyBuiltinFunction )
//      {
//        PyBuiltinFunction fn = (PyBuiltinFunction)fnObj;
//        result = fn.__call__(pyArgs);
//      }
//      else // PyType = generator
//      {
//        PyType pyType = (PyType)fnObj;
//        PyObject obj = pyType.__call__(pyArgs);
//        // TODO: what if the result is not iterable?
//        PyObject iter = obj.__iter__();
//        JsonParser parser = new JsonParser();
//        SpillJArray arr = new SpillJArray();
//        while( (result = iter.__iternext__()) != null )
//        {
//          Item item = parser.parse(result.toString()); // TODO: make a faster conversion between python and jaql
//          iter.__iternext__();
//          arr.add(item);
//        }
//        return new Item(arr);
//      }
//      
//      JsonParser parser = new JsonParser();
//      Item item = parser.parse(result.toString()); // TODO: make a faster conversion between python and jaql
//      return item;
//    }
//    else
//    {
//      throw new RuntimeException("unknown function: "+fnVal);
//    }
  }

  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    Function fn = (Function)exprs[0].eval(context);
    if( fn == null )
    {
      return JsonIterator.NULL;
    }
    if( fn instanceof JaqlFunction  )
    {
      fn = context.getCallable(this, (JaqlFunction)fn);
    }
    fn.setArguments(exprs, 1, exprs.length - 1, true);
    return fn.iter(context);
  }
  
  
  // -- helper classes ----------------------------------------------------------------------------
  
  /** Pseudo-expression used for parameters that do not have a name */
  public static final class NoNameExpr extends Expr
  {
    public NoNameExpr(Expr ... exprs)
    {
      super();
    }
    
    @Override
    public Map<ExprProperty, Boolean> getProperties()
    {
      Map<ExprProperty, Boolean> result = ExprProperty.createUnsafeDefaults();
      result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
      return result;
    }
    
    @Override
    public JsonValue eval(Context context) 
    {
      return null; // means positional
    }
    
    @Override
    public JsonValue compileTimeEval()
    {
      return null;
    }
  }

}
