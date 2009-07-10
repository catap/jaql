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

import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * raise a number to power
 */
@JaqlFn(fnName = "pow", minArgs = 2, maxArgs = 2)
public class PowFn extends Expr
{
  /**
   * @param exprs
   */
  public PowFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr1
   */
  public PowFn(Expr expr0, Expr expr1)
  {
    super(new Expr[]{expr0,expr1});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonNumeric eval(Context context) throws Exception
  {
    JsonValue value1 = exprs[0].eval(context);
    if (value1 == null)
    {
      return null;
    }
    JsonValue value2 = exprs[1].eval(context);
    if (value2 == null)
    {
      return null;
    }

    if (value1 instanceof JsonDouble)
    {
      double x = ((JsonNumeric)value1).doubleValue();
      double y = ((JsonNumeric)value2).doubleValue();
      double z = Math.pow(x, y);
      return new JsonDouble(z);
    }
    
    // TODO: How I hate Java's decimal support... get better decimal pow...
    if( value2 instanceof JsonLong )
    {
      long y = ((JsonLong)value2).get();
      if( y >= Integer.MIN_VALUE && y <= Integer.MAX_VALUE )
      {
        BigDecimal x = ((JsonNumber)value1).decimalValue();
        BigDecimal z = x.pow((int)y, MathContext.DECIMAL128);
        return new JsonDecimal(z);
      }
    }
    // Compute approximately using double because Java's Decimal128 support is so limited!
    double x = ((JsonNumeric)value1).doubleValue();
    double y = ((JsonNumeric)value2).doubleValue();
    double z = Math.pow(x, y);
    return new JsonDecimal(new BigDecimal(z, MathContext.DECIMAL128));
  }
}
