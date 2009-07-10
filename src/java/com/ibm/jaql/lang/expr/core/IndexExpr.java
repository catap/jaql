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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;

/**
 * element(array, index) is the same as array[index], but it captures a simpler 
 * case that doesn't use path expressions. array[index] is transformed to use the
 * element function for better performance.
 */
@JaqlFn(fnName="index", minArgs=2, maxArgs=2)
public class IndexExpr extends Expr // TODO: rename to IndexFn
{
  public IndexExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * array[index] (exprs[0])[ exprs[1] ]
   * 
   * @param array
   * @param index
   */
  public IndexExpr(Expr array, Expr index)
  {
    super(new Expr[]{array, index});
  }

  /**
   * @param expr
   * @param i
   */
  public IndexExpr(Expr expr, int i)
  {
    this(expr, new ConstExpr(JsonLong.makeShared(i)));
  }

  @Override
  public Schema getSchema()
  {
    if (exprs[1] instanceof ConstExpr)
    {
      ConstExpr c = (ConstExpr) exprs[1];
      JsonLong i = new JsonLong(((JsonNumber)c.value).longValueExact());
      Schema inputSchema = exprs[0].getSchema();
      Schema result = exprs[0].getSchema().element(i);
      if (result == null) 
      {
        result = SchemaFactory.anyOrNullSchema();
      }
      else if (inputSchema.hasElement(i).maybeNot()) 
      {
        result = SchemaTransformation.addNullability(result);
      }
      return result;
    }
    return SchemaFactory.anyOrNullSchema();
  }
  
  /**
   * @return
   */
  public final Expr arrayExpr()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final Expr indexExpr()
  {
    return exprs[1];
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
//    // TODO: use proper function?
//    exprText.print("(");
//    exprs[0].decompile(exprText, capturedVars);
//    exprText.print(")[");
//    exprs[1].decompile(exprText, capturedVars);
//    exprText.print("]");
//  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(final Context context) throws Exception
  {
    // TODO: support multiple indexes? $a[3 to 7], $a[ [3,4,5,6,7] ]
    // TODO: support array slices?  $a[3:7]
    JsonValue value;
    JsonValue w = exprs[1].eval(context);
    if (w == null)
    {
      return null;
    }
    long i = ((JsonNumber) w).longValueExact();
    Expr arrayExpr = exprs[0];
    if (arrayExpr.getSchema().isArrayOrNull().always())
    {
      JsonIterator iter = arrayExpr.iter(context);
      boolean hasNext = iter.moveN(i+1);
      value = hasNext ? iter.current() : null;
    }
    else
    {
      value = arrayExpr.eval(context);
      JsonArray array = (JsonArray) value;
      if (array == null)
      {
        return null;
      }
      value = array.nth(i);
    }
    return value;
  }
}
