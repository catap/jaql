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

// TODO: make append macro?

/**
 * append($a, $b, ...) ==> unnest [ $a, $b, ... ] NOT when $a or $b are
 * non-array (and non-null), but that's probably an improvement. NOT when $a or
 * $b are null, but the change to unnest to remove nulls will fix that should
 * append(null, null) be null? it would break any unnest definition... Push
 * unnest into ListExpr?
 */
@JaqlFn(fnName = "append", minArgs = 1, maxArgs = Expr.UNLIMITED_EXPRS)
public class AppendFn extends IterExpr
{
  /**
   * append(array1, array2, ...)
   * 
   * @param exprs
   */
  public AppendFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr1
   * @param expr2
   */
  public AppendFn(Expr expr1, Expr expr2)
  {
    this(new Expr[]{expr1, expr2});
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
      int  index = 0;
      Iter iter  = Iter.empty;

      public Item next() throws Exception
      {
        while (true)
        {
          Item item = iter.next();
          if (item != null)
          {
            return item;
          }
          if (index >= exprs.length)
          {
            return null;
          }
          iter = exprs[index].iter(context);
          index++;
        }
      }
    };
  }
}
