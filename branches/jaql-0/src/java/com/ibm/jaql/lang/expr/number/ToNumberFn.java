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

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonDecimal;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class ToNumberFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("toNumber", ToNumberFn.class);
    }
  }
  
  protected MutableJsonLong jlong = new MutableJsonLong();
  protected MutableJsonDecimal jdec = new MutableJsonDecimal();
  
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
  public JsonNumber eval(final Context context) throws Exception
  {
    JsonValue w = exprs[0].eval(context);
    if (w == null || w instanceof JsonNumber)
    {
      return (JsonNumber)w;
    }

    JsonNumber num;
    if (w instanceof JsonString)
    {
      // TODO: long vs decimal...
      String s = ((JsonString) w).toString();
      try
      {
        long x = Long.parseLong(s);
        jlong.set(x);
        num = jlong;
      }
      catch(Exception e)
      {
        BigDecimal x = new BigDecimal(s, MathContext.DECIMAL128);
        jdec.set(x);
        num = jdec;
      }
    }
    else if (w instanceof JsonBool)
    {
      if (((JsonBool) w).get())
      {
        return JsonLong.ONE;
      }
      else
      {
        return JsonLong.ZERO;
      }
    }
    else
    {
      throw new RuntimeException("cannot cast " + w.getClass().getName()
          + " to number");
    }
    return num;
  }
}
