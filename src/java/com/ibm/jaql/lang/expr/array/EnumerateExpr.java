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
package com.ibm.jaql.lang.expr.array;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
@JaqlFn(fnName = "enumerate", minArgs = 1, maxArgs = 1)
public final class EnumerateExpr extends IterExpr
{
  /**
   * Expr array
   * 
   * @param exprs
   */
  public EnumerateExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param arrayExpr
   */
  public EnumerateExpr(Expr arrayExpr)
  {
    this(new Expr[]{arrayExpr});
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
    // return exprs[0].isNull();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonIterator iter = exprs[0].iter(context);
    final BufferedJsonArray pair = new BufferedJsonArray(2);
    final JsonLong counter = new JsonLong(-1);
    pair.set(0, counter);

    return new JsonIterator(pair) {
      public boolean moveNext() throws Exception
      {
        if (!iter.moveNext()) {
          return false;
        }
        counter.value++;
        pair.set(1, iter.current());
        return true;
      }
    };
  }

}
