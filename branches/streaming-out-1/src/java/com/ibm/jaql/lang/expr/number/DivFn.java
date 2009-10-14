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

import static com.ibm.jaql.json.type.JsonType.NULL;

import java.math.BigDecimal;
import java.math.MathContext;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class DivFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("div", DivFn.class);
    }
  }
  
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
  public JsonNumber eval(final Context context) throws Exception
  {
    JsonValue w1 = exprs[0].eval(context);
    if (w1 == null)
    {
      return null;
    }
    JsonValue w2 = exprs[1].eval(context);
    if (w2 == null)
    {
      return null;
    }

    boolean long1 = w1 instanceof JsonLong;
    boolean long2 = w2 instanceof JsonLong;
    long mod;
    if (long1 && long2)
    {
      mod = ((JsonLong) w1).get() / ((JsonLong) w2).get();
    }
    else
    {
      BigDecimal x1 = long1
          ? new BigDecimal(((JsonLong) w1).get())
          : ((JsonDecimal) w1).get();
      BigDecimal x2 = long2
          ? new BigDecimal(((JsonLong) w2).get())
          : ((JsonDecimal) w2).get();
      mod = x1.divideToIntegralValue(x2, MathContext.DECIMAL128).longValue(); // TODO: does this fit?
    }
    return new JsonLong(mod); // TODO: memory
  }
  
  @Override
  public Schema getSchema()
  {
    Schema in1 = exprs[0].getSchema();
    Schema in2 = exprs[1].getSchema();
    boolean nullable = in1.is(NULL).maybe() || in2.is(NULL).maybe();
    return nullable ? SchemaFactory.longOrNullSchema() : SchemaFactory.longSchema();
  }
}
