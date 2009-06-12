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

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
public class ArrayExpr extends IterExpr
{
  // Runtime state:
  protected BufferedJsonArray tuple;
  
  
  /**
   * @param exprs
   */
  public ArrayExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   */
  public ArrayExpr()
  {
    super(NO_EXPRS);
  }

  /**
   * @param expr
   */
  public ArrayExpr(Expr expr)
  {
    super(new Expr[]{expr});
  }

  /**
   * @param expr0
   * @param expr1
   */
  public ArrayExpr(Expr expr0, Expr expr1)
  {
    super(new Expr[]{expr0, expr1});
  }

  /**
   * @param exprs
   */
  public ArrayExpr(ArrayList<Expr> exprs)
  {
    super(exprs.toArray(new Expr[exprs.size()]));
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
  
  public Schema getSchema()
  {
    // TODO: cache?
    Schema[] schemata = new Schema[exprs.length]; 
    for (int i=0; i<exprs.length; i++)
    {
      schemata[i] = exprs[i].getSchema();
    }
    return new ArraySchema(schemata);
  }
  
  /**
   * 
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isEmpty()
   */
  @Override
  public Bool3 isEmpty()
  {
    return Bool3.valueOf(this.numChildren() == 0);
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
    exprText.print("[");
    int n = exprs.length;
    String sep = "";
    for (int i = 0; i < n; i++)
    {
      exprText.print(sep);
      exprs[i].decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print("]");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception // TODO: generalize for other tuple-like exprs?
  {
    if (exprs.length == 0)
    {
      return JsonArray.EMPTY;
    }
    if( tuple == null )
    {
      tuple = new BufferedJsonArray(exprs.length);
    }
    for (int i = 0; i < exprs.length; i++)
    {
      JsonValue value = exprs[i].eval(context);
      tuple.set(i, value);
    }
    return tuple;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    if (exprs.length == 0)
    {
      return JsonIterator.EMPTY;
    }
    return new JsonIterator() {
      int i = 0;

      public boolean moveNext() throws Exception
      {
        while (true)
        {
          if (i == exprs.length)
          {
            return false;
          }
          Expr expr = exprs[i++];
          currentValue = expr.eval(context);
          return true;
        }
      }
    };
  }
}
