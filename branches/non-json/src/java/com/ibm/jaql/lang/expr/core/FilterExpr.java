/*
 * Copyright (C) IBM Corp. 2009.
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
import java.util.HashSet;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import static com.ibm.jaql.json.type.JsonType.*;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;


public final class FilterExpr extends IterExpr
{
  /**
   * BindingExpr inExpr, Expr predicate
   * 
   * @param exprs
   */
  public FilterExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param inBinding
   * @param predicate
   */
  public FilterExpr(BindingExpr inBinding, Expr predicate)
  {
    super(inBinding, predicate);
  }

  /**
   * @param mapVar
   * @param inExpr
   * @param predicate
   */
  public FilterExpr(Var mapVar, Expr inExpr, Expr predicate)
  {
    super(new BindingExpr(BindingExpr.Type.IN, mapVar, null, inExpr),
        predicate);
  }

  /**
   * @return
   */
  public BindingExpr binding()
  {
    return (BindingExpr) exprs[0];
  }

  /**
   * @return
   */
  public Var var()
  {
    return binding().var;
  }

  /**
   * @return
   */
  public Expr predicate()
  {
    return exprs[1];
  }

  @Override
  public Schema getSchema()
  {
    // inSchema is an array of the values that are to be filtered
    Schema inSchema = binding().getSchema();  
     
    // handle null/empty input
    if (inSchema.isEmpty(ARRAY,NULL).always())
    {
      return SchemaFactory.emptyArraySchema();
    }
    
    // handle non-empty input
    Schema value = inSchema.elements();
    return new ArraySchema(value, null, inSchema.maxElements());
  }
  
  /**
   * 
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    if( i == 0 )
    {
      return Bool3.TRUE;
    }
    return Bool3.FALSE;
  }

  /**
   * This expression can be applied in parallel per partition of child i.
   */
  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
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
    BindingExpr b = binding();
    b.inExpr().decompile(exprText, capturedVars);
    exprText.print("\n-> filter each ");
    exprText.print(b.var.name());
    exprText.print(" ");
    predicate().decompile(exprText, capturedVars);
    capturedVars.remove(b.var);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    final BindingExpr inBinding = binding();
    final Expr pred = predicate();
    final JsonIterator inIter = inBinding.iter(context);

    return new JsonIterator() {
      public boolean moveNext() throws Exception
      {
        while (true)
        {
          if (inIter.moveNext()) {
            if( JaqlUtil.ebv(pred.eval(context)) )
            {
              currentValue = inIter.current();
              return true;
            }
          } 
          else 
          {
            return false;
          }          
        }
      }
    };
  }

}
