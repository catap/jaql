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
package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "min", minArgs = 1, maxArgs = 1)
public class MinAgg extends Expr
{
  /**
   * @param exprs
   */
  public MinAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    Iter iter = exprs[0].iter(context);
    if (iter.isNull())
    {
      return Item.nil;
    }
    Item item;
    do
    {
      item = iter.next();
      if (item == null)
      {
        return Item.nil;
      }
    } while (item.isNull());

    Item min = new Item(); // TODO: memory
    min.copy(item);
    while ((item = iter.next()) != null)
    {
      if (!item.isNull() && item.compareTo(min) < 0)
      {
        min.copy(item);
      }
    }
    return min;
  }
}
