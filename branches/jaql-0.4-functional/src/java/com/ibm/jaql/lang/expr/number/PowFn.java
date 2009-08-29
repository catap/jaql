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
package com.ibm.jaql.lang.expr.number;

import java.math.BigDecimal;
import java.math.MathContext;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.expr.core.MathExpr;

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
  public JsonNumber eval(Context context) throws Exception
  {
    JsonNumber value1 = (JsonNumber)exprs[0].eval(context);
    if (value1 == null)
    {
      return null;
    }
    JsonNumber value2 = (JsonNumber)exprs[1].eval(context);
    if (value2 == null)
    {
      return null;
    }

    JsonType type = MathExpr.promote(value1, value2);
    switch (type)
    {
    case LONG:
      return new JsonLong(pow(value1.longValue(), value2.longValue()));
    case DOUBLE:
      return new JsonDouble(Math.pow(value1.doubleValue(), value2.doubleValue()));
    case DECFLOAT:
      // TODO: How I hate Java's decimal support... get better decimal pow...
      // Compute approximately using double because Java's Decimal128 support is so limited!
      double x = value1.doubleValue();
      double y = value2.doubleValue();
      double z = Math.pow(x, y);
      return new JsonDecimal(new BigDecimal(z, MathContext.DECIMAL128));
    }
    throw new IllegalStateException("cannot happen");
  }
  
  /** Compute a long power */
  public static long pow(long base, long exponent)
  {
    if (exponent < 0) throw new IllegalArgumentException("negative exponent not allowed for long powers");
    
    long power = 1;    
    while (exponent > 0) {
      // if exponent is odd, multiply by base
      if ((exponent & 1L) == 1L) 
      {
        power *= base;
      }
      
      // square base and divide exponent by 2
      base *= base;
      exponent /= 2;
    }
    return power;
  }
  
  @Override
  public Schema getSchema()
  {
    Schema in1 = exprs[0].getSchema();
    Schema in2 = exprs[1].getSchema();
    Schema out = MathExpr.promote(in1, in2);
    if (out == null)
    {
      throw new RuntimeException("pow expects numbers as input");
    }
    return out;
  }
}
