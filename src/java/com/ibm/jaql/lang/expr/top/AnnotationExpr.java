/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang.expr.top;

import java.util.HashSet;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.util.FastPrinter;

/**
 * An annotation that will be placed on an expression.  
 */
public class AnnotationExpr extends Expr
{
  public AnnotationExpr(Expr... exprs)
  {
    super(exprs);
    assert exprs[0] instanceof RecordExpr;
  }

  public AnnotationExpr(RecordExpr anno, Expr expr)
  {
    super(anno, expr);
  }

  @Override
  public Schema getSchema()
  {
    return exprs[1].getSchema();
  }
  
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    assert exprs[0] instanceof RecordExpr || 
         ( exprs[0] instanceof ConstExpr && 
             ((ConstExpr)exprs[0]).value instanceof JsonRecord );
    exprText.print("@");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print("(");
    exprs[1].decompile(exprText, capturedVars);
    exprText.print(")");
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    return exprs[0].eval(context);
  }
  
  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    return exprs[0].iter(context);
  }

}
