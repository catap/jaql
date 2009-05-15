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
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "pairwise", minArgs = 2, maxArgs = Expr.UNLIMITED_EXPRS)
public class PairwiseFn extends IterExpr
{
  /**
   * @param exprs
   */
  public PairwiseFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonIterator[] iters = new JsonIterator[exprs.length];
    for (int i = 0; i < exprs.length; i++)
    {
      iters[i] = exprs[i].iter(context);
    }

    final BufferedJsonArray tuple = new BufferedJsonArray(exprs.length); // TODO: memory
    return new JsonIterator(tuple) {
      public boolean moveNext() throws Exception
      {
        boolean foundOne = false;
        for (int i = 0; i < exprs.length; i++)
        {
          JsonIterator iter = iters[i];
          if (iter.moveNext())
          {
            foundOne = true;
            tuple.set(i, iter.current());
          }
          else
          {
            tuple.set(i, null);
          }
        }
        if (foundOne)
        {
          return true; // currentValue == tuple
        }
        return false;
      }
    };
  }
}
