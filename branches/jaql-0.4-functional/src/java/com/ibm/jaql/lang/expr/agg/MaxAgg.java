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
package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public final class MaxAgg extends AlgebraicAggregate
{
  private JsonValue max;

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("max", MaxAgg.class);
    }
  }
  
  /**
   * @param exprs
   */
  public MaxAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public MaxAgg(Expr expr)
  {
    super(expr);
  }

  @Override
  public void init(Context context) throws Exception
  {
    max = null;
  }

  @Override
  public void accumulate(JsonValue value) throws Exception
  {
    if( value == null )
    {
      return;
    }
    if( max == null )
    {
      max = value.getCopy(null);
    }
    else if( value.compareTo(max) > 0 )
    {
      max = value.getCopy(max);
    }
  }

  @Override
  public JsonValue getPartial() throws Exception
  {
    return max;
  }

  @Override
  public void combine(JsonValue value) throws Exception
  {
    accumulate(value);
  }

  @Override
  public JsonValue getFinal() throws Exception
  {
    return max;
  }
  
  @Override 
  public Schema getPartialSchema()
  {
    return getSchema();
  }
  
  @Override
  public Schema getSchema()
  {
    Schema in = exprs[0].getSchema();
    Schema out = SchemaTransformation.arrayElements(in);
    if (out == null)
    {
      if (in.isEmpty(JsonType.ARRAY, JsonType.NULL).maybe())
      {
        return SchemaFactory.nullSchema();
      }
      throw new RuntimeException("max aggregate expects array input");
    }
    return SchemaTransformation.addNullability(out);
  }

}
