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
package com.ibm.jaql.lang.expr.nil;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class FirstNonNullFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par0u
  {
    public Descriptor()
    {
      super("firstNonNull", FirstNonNullFn.class);
    }
  }
  
  /**
   * item firstNonNull(...)
   * 
   * @param exprs
   */
  public FirstNonNullFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr0
   * @param expr1
   */
  public FirstNonNullFn(Expr expr0, Expr expr1)
  {
    super(new Expr[]{expr0, expr1});
  }
  
  @Override
  public Schema getSchema()
  {
    if( exprs.length == 0 )
    {
      return SchemaFactory.nullSchema();
    }
    Schema result = exprs[0].getSchema();
    if( result.is(JsonType.NULL).never() )
    {
      return result;
    }
    result = SchemaTransformation.removeNullability(result);
    for( int i = 1; i < exprs.length; i++ )
    {
      Schema s = exprs[i].getSchema();
      Schema t = SchemaTransformation.removeNullability(s);
      // TODO: use exact schema?
      // result = OrSchema.make(result, t);
      result = SchemaTransformation.merge(result, t);
      if( s.is(JsonType.NULL).never() )
      {
        return result;
      }
    }
    result = SchemaTransformation.addNullability(result);
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(final Context context) throws Exception
  {
    for (int i = 0; i < exprs.length; i++)
    {
      JsonValue value = exprs[i].eval(context);
      if (value != null)
      {
        return value;
      }
    }
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    for (int i = 0; i < exprs.length; i++)
    {
      JsonIterator iter = exprs[i].iter(context);
      if (!iter.isNull())
      {
        return iter;
      }
    }
    return JsonIterator.NULL;
  }
}
