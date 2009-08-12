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

import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;

/**
 * 
 */
@JaqlFn(fnName="range", minArgs=2, maxArgs=2)
public class RangeExpr extends IterExpr
{
  /**
   * @param exprs
   */
  public RangeExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * expr0 to expr1
   * 
   * @param expr0
   * @param expr1
   */
  public RangeExpr(Expr expr0, Expr expr1)
  {
    super(new Expr[]{expr0, expr1});
  }
  
  public Schema getSchema()
  {
    return SchemaFactory.arraySchema();
  }

//  /*
//   * (non-Javadoc)
//   * 
//   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
//   *      java.util.HashSet)
//   */
//  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
//      throws Exception
//  {
//    exprText.print(" (");
//    exprs[0].decompile(exprText, capturedVars);
//    exprText.print(") to (");
//    exprs[1].decompile(exprText, capturedVars);
//    exprText.print(") ");
//  }

  @Override
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    if (exprs[0] instanceof ConstExpr && exprs[1] instanceof ConstExpr)
    {
      // We only consider small ranges as a constant.
      long start = ((JsonLong) ((ConstExpr) exprs[0]).value).get();
      long end = ((JsonLong) ((ConstExpr) exprs[1]).value).get();
      if (end - start < 10)
      {
        result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
      }
    }
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonNumeric v1 = (JsonNumeric) exprs[0].eval(context);
    if (v1 == null)
    {
      return JsonIterator.NULL;
    }
    JsonNumeric v2 = (JsonNumeric) exprs[1].eval(context);
    if (v2 == null)
    {
      return JsonIterator.NULL;
    }
    final long start = v1.longValueExact();
    final long end = v2.longValueExact();

    return new JsonIterator(new MutableJsonLong(start - 1)) {
      public boolean moveNext()
      {
        MutableJsonLong num = (MutableJsonLong)currentValue;
        if (num.get() + 1 <= end)
        {
          num.set(num.get()+1);
          return true;
        }
        return false;
      }
    };
  }
}
