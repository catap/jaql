/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.lang.expr.numeric;

import java.math.BigDecimal;
import java.math.MathContext;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JDecimal;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "div", minArgs = 2, maxArgs = 2)
public class DivFn extends Expr
{
  /**
   * @param exprs
   */
  public DivFn(Expr[] exprs)
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
    JValue w1 = exprs[0].eval(context).get();
    if (w1 == null)
    {
      return Item.nil;
    }
    JValue w2 = exprs[1].eval(context).get();
    if (w2 == null)
    {
      return Item.nil;
    }

    boolean long1 = w1 instanceof JLong;
    boolean long2 = w2 instanceof JLong;
    long mod;
    if (long1 && long2)
    {
      mod = ((JLong) w1).value / ((JLong) w2).value;
    }
    else
    {
      BigDecimal x1 = long1
          ? new BigDecimal(((JLong) w1).value)
          : ((JDecimal) w1).value;
      BigDecimal x2 = long2
          ? new BigDecimal(((JLong) w2).value)
          : ((JDecimal) w2).value;
      mod = x1.divideToIntegralValue(x2, MathContext.DECIMAL128).longValue(); // TODO: does this fit?
    }
    return new Item(new JLong(mod)); // TODO: memory
  }
}
