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
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "replaceElement", minArgs = 3, maxArgs = 3)
public class ReplaceElementFn extends IterExpr
{
  /**
   * replaceElement(array, index, value)
   * 
   * @param exprs
   */
  public ReplaceElementFn(Expr[] exprs)
  {
    super(exprs);
  }
  
  public ReplaceElementFn(Expr array, Expr index, Expr value)
  {
    super(new Expr[]{array, index, value});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Iter iter(final Context context) throws Exception
  {
    final Iter tmpIter = exprs[0].iter(context);
    final JLong replaceIndexLong = (JLong) exprs[1].eval(context).get();
    if (replaceIndexLong == null || replaceIndexLong.value < 0)
    {
      return tmpIter;
    }
    return new Iter() {
      long index        = 0;
      long replaceIndex = replaceIndexLong.value;
      Iter iter         = tmpIter;

      public Item next() throws Exception
      {
        long i = index;
        Item item = iter.next();
        index++;
        if (i == replaceIndex)
        {
          return exprs[2].eval(context);
        }
        if (item != null)
        {
          return item;
        }
        if (i >= replaceIndex)
        {
          return null;
        }
        iter = Iter.empty;
        return Item.NIL;
      }
    };
  }
}
