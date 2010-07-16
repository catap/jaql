/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonDouble;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;


/**
 * Perform exponential smoothing on a sequence of numbers:
 *   s[0] = x[0]
 *   s[i] = a * x[i] + (1-a) * s[i-1]
 * The numbers are cast to a double and the result is always a double.
 */
public final class ExpSmoothAgg extends Aggregate
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("expSmooth", ExpSmoothAgg.class);
    }
  }
  
  protected double alpha;
  protected double avg;
  protected MutableJsonDouble result; 
  
  /**
   * @param exprs
   */
  public ExpSmoothAgg(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.doubleOrNullSchema();
  }
  
  @Override
  public void init(Context context) throws Exception
  {
    result = null;
    alpha = ((JsonNumber)exprs[1].eval(context)).doubleValue();
  }

  @Override
  public void accumulate(JsonValue value) throws Exception
  {
    double d = ((JsonNumber)value).doubleValue();
    if( result == null )
    {
      result = new MutableJsonDouble(d);
      avg = d;
    }
    else
    {
      avg = alpha * d + (1 - alpha) * avg; 
    }
  }

  @Override
  public JsonValue getFinal() throws Exception
  {
    if( result != null )
    {
      result.set( avg );
    }
    return result;
  }
}
