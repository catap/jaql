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
import java.util.HashSet;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;

// 

//FIXME: always represent as "unnest for"?
/**
 * 6/20/08: unnest now removes nulls
 */
public class UnnestExpr extends IterExpr
{
  /**
   * @param exprs
   */
  public UnnestExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public UnnestExpr(Expr expr)
  {
    super(new Expr[]{expr});
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
    exprText.print("unnest (");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonIterator iter = exprs[0].iter(context);
    if (iter.isNull())
    {
      return JsonIterator.NULL;
    }

    return new JsonIterator() {
      JsonIterator inner = JsonIterator.EMPTY;

      public boolean moveNext() throws Exception
      {
        while (true)
        {
          if (inner.moveNext()) {
            currentValue = inner.current();
            return true;
          }
          if (!iter.moveNext()) {
            return false;
          }
          JsonValue w = iter.current();
          if (w instanceof JsonArray)
          {
            inner = ((JsonArray) w).iter();
          }
          else
          {
            inner = JsonIterator.EMPTY;
            currentValue = w;
            return true;
          }
        }
      }
    };
  }
}
