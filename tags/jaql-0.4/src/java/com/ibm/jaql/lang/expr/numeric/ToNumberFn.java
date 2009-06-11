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
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.json.type.JDecimal;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JNumber;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "number", minArgs = 1, maxArgs = 1)
public class ToNumberFn extends Expr
{
  protected JLong jlong = new JLong();
  protected JDecimal jdec = new JDecimal();
  protected Item result = new Item();
  
  /**
   * @param exprs
   */
  public ToNumberFn(Expr[] exprs)
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
    Item item = exprs[0].eval(context);
    JValue w = item.get();
    if (w == null || w instanceof JNumber)
    {
      return item;
    }

    JNumber num;
    if (w instanceof JString)
    {
      // TODO: long vs decimal...
      String s = ((JString) w).toString();
      try
      {
        long x = Long.parseLong(s);
        jlong.setValue(x);
        num = jlong;
      }
      catch(Exception e)
      {
        BigDecimal x = new BigDecimal(s, MathContext.DECIMAL128);
        jdec.setValue(x);
        num = jdec;
      }
    }
    else if (w instanceof JBool)
    {
      if (((JBool) w).getValue())
      {
        return JLong.ONE_ITEM;
      }
      else
      {
        return JLong.ZERO_ITEM;
      }
    }
    else
    {
      throw new RuntimeException("cannot cast " + w.getClass().getName()
          + " to number");
    }
    result.set(num);
    return result;
  }
}
