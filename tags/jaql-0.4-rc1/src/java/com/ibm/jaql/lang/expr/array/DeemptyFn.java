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
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "deempty", minArgs = 1, maxArgs = 1)
public class DeemptyFn extends IterExpr
{
  /**
   * @param exprs
   */
  public DeemptyFn(Expr[] exprs)
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
    final Iter iter = exprs[0].iter(context);
    if (iter.isNull())
    {
      return Iter.nil;
    }

    return new Iter() {
      public Item next() throws Exception
      {
        while (true)
        {
          Item item = iter.next();
          if (item == null)
          {
            return null;
          }
          JValue w = item.get();
          if (w != null)
          {
            if (w instanceof JRecord)
            {
              if (((JRecord) w).arity() > 0)
              {
                return item;
              }
            }
            else if (w instanceof JArray)
            {
              if (!((JArray) w).isEmpty())
              {
                return item;
              }
            }
            else
            {
              return item;
            }
          }
        }
      }
    };
  }
}
