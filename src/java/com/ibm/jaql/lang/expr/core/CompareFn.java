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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.lang.core.Context;

/**
 * 
 */
public @JaqlFn(fnName = "compare", minArgs = 2, maxArgs = 2)
class CompareFn extends Expr
{
  /**
   * @param exprs
   */
  public CompareFn(Expr[] exprs)
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
    Item item1 = exprs[0].eval(context);
    Item item2 = exprs[1].eval(context);
    int cmp = item1.compareTo(item2);
    if (cmp < 0)
    {
      return JLong.MINUS_ONE_ITEM;
    }
    else if (cmp == 0)
    {
      return JLong.ZERO_ITEM;
    }
    else
    {
      return JLong.ONE_ITEM;
    }
  }
}
