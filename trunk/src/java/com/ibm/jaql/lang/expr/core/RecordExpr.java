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

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.nil.NullElementOnEmptyFn;
import com.ibm.jaql.util.Bool3;

//TODO: add optimized RecordExpr when all cols are known at compile time
/**
 * 
 */
public class RecordExpr extends Expr
{
  protected BufferedJsonRecord record;

  /**
   * every exprs[i] is a FieldExpr
   * 
   * @param exprs
   */
  public RecordExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   */
  public RecordExpr()
  {
    this(NO_EXPRS);
  }
  
  /**
   * 
   * @param expr
   */
  public RecordExpr(Expr expr)
  {
    super(expr);
  }

  /**
   * 
   * @param expr0
   * @param expr1
   */
  public RecordExpr(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }

  /**
   * 
   * @param expr0
   * @param expr1
   * @param expr2
   */
  public RecordExpr(Expr expr0, Expr expr1, Expr expr2)
  {
    super(expr0, expr1, expr2);
  }

  /**
   * 
   * @param fields
   */
  public RecordExpr(ArrayList<FieldExpr> fields)
  {
    super(fields);
  }

  /**
   * Either construct a RecordExpr or some for loops over a RecordExpr
   * to handle flatten requests.
   * 
   * @param args
   * @return
   */
  public static Expr make(Env env, Expr[] args)
  {
    int n = 0;
    for(Expr e: args)
    {
      if( e instanceof NameValueBinding &&
          e.child(1) instanceof FlattenExpr )
      {
        n++;
      }
    }
    if( n == 0 )
    {
      return new RecordExpr(args);
    }

    Var[] vars = new Var[n];
    Expr[] ins = new Expr[n];
    Expr[] doargs = new Expr[n+1];
    n = 0;
    for(int i = 0 ; i < args.length ; i++)
    {
      if( args[i] instanceof NameValueBinding && 
          args[i].child(1) instanceof FlattenExpr )
      {
        Expr flatten = args[i].child(1);
        Var letVar = new Var("$_toflat_"+n);
        vars[n] = env.makeVar("$_flat_"+n);
        ins[n] = new VarExpr(letVar);
        Expr e = flatten.child(0);
        if( e.isEmpty().maybe() )
        {
          e = new NullElementOnEmptyFn(e);
        }
        doargs[n] = new BindingExpr(BindingExpr.Type.EQ, letVar, null, e);
        flatten.replaceInParent(new VarExpr(vars[n]));
        n++;
      }
    }
    Expr e = new ArrayExpr(new RecordExpr(args));
    for( n-- ; n >= 0 ; n-- )
    {
      e = new ForExpr(vars[n], ins[n], e);
    }
    doargs[doargs.length-1] = e;
    e = new DoExpr(doargs);
    return e;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isArray()
   */
  @Override
  public Bool3 isArray()
  {
    return Bool3.FALSE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
   */
  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  /**
   * 
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  /**
   * @return
   */
  public final int numFields()
  {
    return exprs.length;
  }

  /**
   * @param i
   * @return
   */
  public final FieldExpr field(int i)
  {
    return (FieldExpr) exprs[i];
  }

  /**
   * Return the value expression of the field with the given name, if it can be
   * found at compile time. A null means the field was not found at
   * compile-time, but it could still exist at run-time.
   * 
   * @param name
   * @return
   */
  public Expr findStaticFieldValue(JsonString name)
  {
    Expr valExpr = null;
    for (int i = 0; i < exprs.length; i++)
    {
      if (exprs[i] instanceof NameValueBinding)
      {
        NameValueBinding nv = (NameValueBinding) exprs[i];
        Expr nameExpr = nv.nameExpr();
        if (nameExpr instanceof ConstExpr)
        {
          ConstExpr ce = (ConstExpr) nameExpr;
          JsonValue t = ce.value;
          if (t instanceof JsonString)
          {
            if (name.equals(t))
            {
              if (valExpr != null)
              {
                throw new RuntimeException(
                    "duplicate field name in record constructor: " + name);
              }
              valExpr = nv.valueExpr();
            }
          }
        }
      }
    }
    return valExpr;
  }

  /**
   * @param name
   * @return
   */
  public Expr findStaticFieldValue(String name) // TODO: migrate callers to JString version
  {
    return findStaticFieldValue(new JsonString(name)); // TODO: memory
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("{");
    String sep = " ";
    for (int i = 0; i < exprs.length; i++)
    {
      exprText.print(sep);
      exprs[i].decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print(" }");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception
  {
    if (record == null)
    {
      record = new BufferedJsonRecord();
    }
    else
    {
      record.clear();
    }

    for (int i = 0; i < exprs.length; i++)
    {
      FieldExpr f = (FieldExpr) exprs[i];
      f.eval(context, record);
    }
    return record;
  }
}
