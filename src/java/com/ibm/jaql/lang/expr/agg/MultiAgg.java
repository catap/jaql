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

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

// TODO: Prototype tuple aggregate that might go away...

/**
 * 
 */
@JaqlFn(fnName = "multiAgg", minArgs = 0, maxArgs = Expr.UNLIMITED_EXPRS)
public class MultiAgg extends PushAggExpr
{

  /**
   * @param exprs
   */
  public MultiAgg(Expr[] exprs)
  {
    super(exprs);
    for (int i = 0; i < exprs.length; i++)
    {
      if (!(exprs[i] instanceof PushAggExpr))
      {
        throw new RuntimeException("multiAgg requires Push Aggregates");
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.agg.PushAggExpr#init(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public PushAgg init(final Context context) throws Exception
  {
    final PushAgg[] aggs = new PushAgg[exprs.length]; // TODO: memory
    for (int i = 0; i < exprs.length; i++)
    {
      aggs[i] = ((PushAggExpr) exprs[i]).init(context);
    }

    return new PushAgg() {
      @Override
      public void addMore() throws Exception
      {
        for (int i = 0; i < exprs.length; i++)
        {
          aggs[i].addMore();
        }
      }

      @Override
      public Item eval() throws Exception
      {
        FixedJArray arr = new FixedJArray(exprs.length); // TODO: memory
        for (int i = 0; i < exprs.length; i++)
        {
          Item item = aggs[i].eval();
          arr.set(i, item);
        }
        return new Item(arr); // TODO: memory
      }
    };
  }

}
