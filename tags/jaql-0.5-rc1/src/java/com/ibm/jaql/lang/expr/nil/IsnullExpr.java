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
package com.ibm.jaql.lang.expr.nil;

import java.util.HashSet;

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.FastPrinter;

import static com.ibm.jaql.json.type.JsonType.*;

/**
 * 
 */
// @JaqlFn(fnName = "isnull", minArgs = 1, maxArgs = 1)
public class IsnullExpr extends Expr
{
  /**
   * @param exprs
   */
  public IsnullExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public IsnullExpr(Expr expr)
  {
    this(new Expr[]{expr});
  }
  
  @Override
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print(kw("isnull") + " (");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(final Context context) throws Exception
  {
    Expr expr = exprs[0];
    boolean b;
    if (expr.getSchema().is(ARRAY,NULL).always())
    {
      JsonIterator iter = expr.iter(context);
      b = iter.isNull();
    }
    else
    {
      JsonValue value = expr.eval(context);
      b = value == null;
    }
    return JsonBool.make(b);
  }
}
