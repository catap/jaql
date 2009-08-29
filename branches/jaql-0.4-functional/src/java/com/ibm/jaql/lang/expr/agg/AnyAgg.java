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
package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/** Picks any value. If there is at least one non-null values, picks a non-null value.
 * 
 */
public final class AnyAgg extends AlgebraicAggregate
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor
  {
    public Descriptor()
    {
      super(
          "any",
          AnyAgg.class,
          new JsonValueParameters(new JsonValueParameter("a", SchemaFactory.arrayOrNullSchema())),
          SchemaFactory.anySchema());
    }
  }
  
  private JsonValue result;
  
  /**
   * Expr aggInput, Expr N
   * @param exprs
   */
  public AnyAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public AnyAgg(Expr expr)
  {
    super(expr);
  }

  @Override
  public void initInitial(Context context) throws Exception
  {
    result = null;
  }

  @Override
  public void addInitial(JsonValue value) throws Exception
  {
    if( result == null && value != null )
    {
      result = value.getCopy(null);
    }
  }

  @Override
  public JsonValue getPartial() throws Exception
  {
    return result;
  }

  @Override
  public void addPartial(JsonValue value) throws Exception
  {
    if( result == null && value != null )
    {
      result = value.getCopy(null);
    }
  }

  @Override
  public JsonValue getFinal() throws Exception
  {
    return result;
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
      throw new RuntimeException("any aggregate expects array input");
    }
    return out;
  }
}
