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
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.util.Bool3;

//TODO: add optimized RecordExpr when all cols are known at compile time
/**
 * 
 */
public class RecordExpr extends Expr
{
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
  public Expr findStaticFieldValue(JString name)
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
          JValue t = ce.value.get();
          if (t instanceof JString)
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
    return findStaticFieldValue(new JString(name)); // TODO: memory
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
  public Item eval(Context context) throws Exception
  {
    Item result = context.getTemp(this);
    MemoryJRecord rec = (MemoryJRecord) result.get();
    if (rec == null)
    {
      rec = new MemoryJRecord();
      result.set(rec);
    }
    else
    {
      rec.clear();
    }

    for (int i = 0; i < exprs.length; i++)
    {
      FieldExpr f = (FieldExpr) exprs[i];
      f.eval(context, rec);
    }
    return result;
  }
}
