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

import java.math.BigDecimal;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JDecimal;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "sumPA", minArgs = 1, maxArgs = 1)
public class SumPA extends PushAggExpr
{

  /**
   * @param exprs
   */
  public SumPA(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr0
   */
  public SumPA(Expr expr0)
  {
    super(expr0);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.agg.PushAggExpr#init(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public PushAgg init(final Context context) throws Exception
  {
    return new PushAgg() {
      boolean    sawLong = false;
      long       lsum    = 0;
      BigDecimal sum     = null;

      @Override
      public void addMore() throws Exception
      {
        Iter iter = exprs[0].iter(context);
        Item item;
        while ((item = iter.next()) != null)
        {
          JValue w = item.get();
          if (w instanceof JLong)
          {
            sawLong = true;
            lsum += ((JLong) w).value;
          }
          else if (w != null)
          {
            JDecimal n = (JDecimal) w;
            // TODO: need a mutable BigDecimal...
            if (sum == null)
            {
              sum = n.value;
            }
            else
            {
              sum = sum.add(n.value);
            }
          }
        }
      }

      @Override
      public Item eval() throws Exception
      {
        if (sum == null)
        {
          if (sawLong)
          {
            return new Item(new JLong(lsum)); // TODO: memory
          }
          return Item.nil;
        }
        else
        {
          if (lsum != 0)
          {
            sum = sum.add(new BigDecimal(lsum));
          }
          return new Item(new JDecimal(sum)); // TODO: memory
        }
      }
    };
  }

}
