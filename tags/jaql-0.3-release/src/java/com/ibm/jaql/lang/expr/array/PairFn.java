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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

// TODO: introduce macro exprs that always rewrite into something else
/**
 * pair(A,B) == [A,B]
 */
@JaqlFn(fnName = "pair", minArgs = 2, maxArgs = 2)
public class PairFn extends IterExpr // TODO: rewrite into [A,B]
{
  /**
   * [ exprs[0], exprs[1] ]
   * 
   * @param exprs
   */
  public PairFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr0
   * @param expr1
   */
  public PairFn(Expr expr0, Expr expr1)
  {
    super(new Expr[]{expr0, expr1});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Iter iter(final Context context) throws Exception
  {
    return new Iter() {
      int index = 0;

      public Item next() throws Exception
      {
        if (index < exprs.length)
        {
          Item item = exprs[index].eval(context);
          index++;
          return item;
        }
        return null;
      }
    };
  }
}
