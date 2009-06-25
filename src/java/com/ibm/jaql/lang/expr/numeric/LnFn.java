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
 * natural logarithm
 */
@JaqlFn(fnName = "ln", minArgs = 1, maxArgs = 1)
public class LnFn extends Expr
{
  /**
   * @param exprs
   */
  public LnFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr1
   */
  public LnFn(Expr expr1)
  {
    super(new Expr[]{expr1});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(Context context) throws Exception
  {
    JValue item1 = exprs[0].eval(context).get();
    if (item1 == null)
    {
      return Item.nil;
    }
    BigDecimal n1, n2;
    if (item1 instanceof JLong)
    {
      n1 = new BigDecimal(((JLong) item1).value);
    }
    else
    {
      n1 = ((JDecimal) item1).value;
    }
    // TODO: How I hate Java's decimal support... get better decimal log
    n2 = new BigDecimal(Math.log(n1.doubleValue()), MathContext.DECIMAL128);
    return new Item(new JDecimal(n2)); // TODO: reuse
  }
}
