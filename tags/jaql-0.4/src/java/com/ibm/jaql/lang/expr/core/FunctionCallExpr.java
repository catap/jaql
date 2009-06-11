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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.util.Bool3;

// TODO: optimize the case when the fn is known to have a IterExpr body
/**
 * 
 */
public class FunctionCallExpr extends Expr
{
  // protected Item[] args;
  protected Expr[] args;

  /**
   * @param fn
   * @param args
   * @return
   */
  private static Expr[] makeExprs(Expr fn, ArrayList<Expr> args)
  {
    Expr[] exprs = new Expr[args.size() + 1];
    exprs[0] = fn;
    for (int i = 1; i < exprs.length; i++)
    {
      exprs[i] = args.get(i - 1);
    }
    return exprs;
  }

  /**
   * exprs[0](exprs[1:*])
   * 
   * @param exprs
   */
  public FunctionCallExpr(Expr[] exprs)
  {
    super(exprs);
    // args = new Item[exprs.length - 1];
    args = new Expr[exprs.length - 1];
  }
  
  /**
   * @param fn
   * @param args
   */
  public FunctionCallExpr(Expr fn, ArrayList<Expr> args)
  {
    this(makeExprs(fn, args));
  }

  /**
   * 
   * @param fn
   * @param arg0
   */
  public FunctionCallExpr(Expr fn, Expr arg1)
  {
    super(fn,arg1);
  }

  /**
   * 
   * @param fn
   * @param arg1
   * @param arg2
   */
  public FunctionCallExpr(Expr fn, Expr arg1, Expr arg2)
  {
    super(fn,arg1,arg2);
  }

  /**
   * @return
   */
  public final Expr fnExpr()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final int numArgs()
  {
    return exprs.length - 1;
  }

  /**
   * @param i
   * @return
   */
  public final Expr arg(int i)
  {
    return exprs[i + 1];
  }

  @Override
  public Bool3 isArray()
  {
    Expr fn = fnExpr();
    DefineFunctionExpr def = null;
    if( fn instanceof DoExpr )
    {
      fn = ((DoExpr)fn).returnExpr();
    }
    if( fn instanceof DefineFunctionExpr )
    {
      def = (DefineFunctionExpr)fn;
    }
    else if( fn instanceof ConstExpr )
    {
      JFunction jf = (JFunction)((ConstExpr)fn).value.get();
      if( fn != null )
      {
        def = jf.getFunction();
      }
    }
    if( def != null )
    {
      return def.body().isArray();
    }
    return Bool3.UNKNOWN;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(")");
    exprText.print("(");
    char sep = ' ';
    for (int i = 1; i < exprs.length; i++)
    {
      exprText.print(sep);
      exprText.print("(");
      exprs[i].decompile(exprText, capturedVars);
      exprText.print(")");
      sep = ',';
    }
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Item eval(Context context) throws Exception
  {
    JValue fnVal = exprs[0].eval(context).get();
    if( fnVal == null )
    {
      return Item.NIL;
    }
    // if( fnVal instanceof JFunction )
    {
      JFunction fn = context.getCallable(this, (JFunction)fnVal);
      return fn.eval(context, exprs, 1, exprs.length - 1);
    }
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
  public Iter iter(Context context) throws Exception
  {
    JValue fnVal = exprs[0].eval(context).get();
    if (fnVal == null)
    {
      return Iter.nil;
    }
    JFunction fn = context.getCallable(this, (JFunction)fnVal);
    return fn.iter(context, exprs, 1, exprs.length - 1);
  }
}
