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

import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * raise base of natural log (e) to arg: e^a pow(x,y) = exp( y * ln(x) )
 */
@JaqlFn(fnName = "exp", minArgs = 1, maxArgs = 1)
public class ExpFn extends Expr
{
  /**
   * @param exprs
   */
  public ExpFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr1
   */
  public ExpFn(Expr expr1)
  {
    super(new Expr[]{expr1});
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
    BigDecimal n1, n2;
    if (value1 instanceof JsonLong)
    {
      n1 = new BigDecimal(((JsonLong) value1).get());
    }
    else
    {
      n1 = ((JsonDecimal) value1).get();
    }
    // TODO: How I hate Java's decimal support... get better decimal exp
    n2 = new BigDecimal(Math.exp(n1.doubleValue()), MathContext.DECIMAL128);
    return new JsonDecimal(n2); // TODO: reuse
  }
}
