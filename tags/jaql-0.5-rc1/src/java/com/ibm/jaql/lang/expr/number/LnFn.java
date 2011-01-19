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
package com.ibm.jaql.lang.expr.number;

import java.math.BigDecimal;
import java.math.MathContext;

import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Return the natural logarithm of a numeric value
 * 
 * Usage:
 * number abs(number)
 * 
 */
public class LnFn extends AbstractRealFn
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("ln", LnFn.class);
    }
  }
  
  public LnFn(Expr ... exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonNumber eval(Context context) throws Exception
  {
    JsonValue value = exprs[0].eval(context);
    if (value == null)
    {
      return null;
    }
    JsonNumber n = (JsonNumber)value;
    
    if (n instanceof JsonDecimal)
    {
      // TODO: How I hate Java's decimal support... get better decimal log
      BigDecimal m = new BigDecimal(Math.log(n.doubleValue()), MathContext.DECIMAL128);
      return new JsonDecimal(m); // TODO: reuse  
    }
    else
    {
      return new JsonDouble(Math.log(n.doubleValue()));
    }
  }
}
