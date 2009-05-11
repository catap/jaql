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

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
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
  public Iter iter(final Context context) throws Exception
  {
    final Iter[] iters = new Iter[exprs.length];
    for (int i = 0; i < exprs.length; i++)
    {
      iters[i] = exprs[i].iter(context);
    }

    return new Iter() {
      FixedJArray tuple  = new FixedJArray(exprs.length); // TODO: memory
      Item        result = new Item(tuple);              // TODO: memory

      public Item next() throws Exception
      {
        boolean foundOne = false;
        for (int i = 0; i < exprs.length; i++)
        {
          Item item = iters[i].next();
          if (item != null)
          {
            foundOne = true;
            tuple.set(i, item);
          }
          else
          {
            tuple.set(i, Item.NIL);
          }
        }
        if (foundOne)
        {
          return result;
        }
        return null;
      }
    };
  }
}
