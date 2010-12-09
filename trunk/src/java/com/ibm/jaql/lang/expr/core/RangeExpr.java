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
package com.ibm.jaql.lang.expr.core;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/**
 * @jaqlDescription Range generates a continuous array of numbers
 * 
 * Usage:
 * range(size)      = [0,size-1]
 * range(size,null) = [0,size-1]
 * range(start,end) = [start,end]
 * range(start,end,skip) = if skip > 0 then for(i = start, i <= end, i += skip)
 *                         else error
 * range(size,null,skip) = if skip > 0 then for(i = 0, i < size, i += skip)
 *                         else error
 */
public class RangeExpr extends IterExpr // TODO: rename to RangeFn
{
  private final static Schema schema = new ArraySchema(null, SchemaFactory.longSchema());
  public static class Descriptor implements BuiltInFunctionDescriptor 
  {
    private JsonValueParameters parameters = new JsonValueParameters(new JsonValueParameter[] {
        new JsonValueParameter("startOrSize", SchemaFactory.longOrNullSchema()),
        new JsonValueParameter("end", SchemaFactory.longOrNullSchema(), null),
        new JsonValueParameter("by", SchemaFactory.longSchema(), JsonLong.ONE),
    });

    @Override
    public Expr construct(Expr[] positionalArgs)
    {
      return new RangeExpr(positionalArgs);
    }

    @Override
    public Class<? extends Expr> getImplementingClass() 
    {
      return RangeExpr.class;
    }

    @Override
    public String getName()
    {
      return "range";
    }

    @Override
    public JsonValueParameters getParameters()
    {
      return parameters;
    }

    @Override
    public Schema getSchema()
    {
      return schema;
    }
  }

  /**
   * @param exprs
   */
  public RangeExpr(Expr... exprs)
  {
    super(exprs);
  }

  public Schema getSchema()
  {
    return schema;
  }

//  @Override
//  public Map<ExprProperty, Boolean> getProperties() 
//  {
//    Map<ExprProperty, Boolean> result = super.getProperties();
//    final long limit = 10;
//    long range;
//    // We only consider small ranges as a constant.
//    if( exprs.length == 1 && 
//        exprs[0] instanceof ConstExpr )
//    {
//      range = ((JsonNumber) ((ConstExpr) exprs[1]).value).longValueExact();
//    }
//    else if (exprs.length == 2 && 
//             exprs[0] instanceof ConstExpr && 
//             exprs[1] instanceof ConstExpr)
//    {
//      long start = ((JsonNumber) ((ConstExpr) exprs[0]).value).longValueExact();
//      long end = ((JsonNumber) ((ConstExpr) exprs[1]).value).longValueExact();
//      range = end - start;
//    }
//    else // could also look at start, end, skip
//    {
//      range = limit + 1;
//    }
//    if( range < 10 )
//    {
//      result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
//    }
//    return result;
//  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonNumber jstartOrSize = (JsonNumber) exprs[0].eval(context);
    if (jstartOrSize == null)
    {
      return JsonIterator.NULL;
    }
    
    JsonNumber jend = (JsonNumber) exprs[1].eval(context);
    final long start;
    final long end;
    if( jend == null ) // range(size)
    {
      start = 0;
      end = jstartOrSize.longValueExact() - 1;
    }
    else // range(start,end)
    {
      start = jstartOrSize.longValueExact(); 
      end = jend.longValueExact();
    }
    
    JsonNumber jskip = (JsonNumber) exprs[2].eval(context);
    final long skip = jskip.longValueExact();
    if( skip <= 0 )
    {
      throw new IllegalArgumentException("skip must be > 0");
    }
    final MutableJsonLong result = new MutableJsonLong(start - 1);

    result.set(start - skip);
    return new JsonIterator(result) 
    {
      public boolean moveNext()
      {
        long next = result.get() + skip;
        if( next <= end )
        {
          result.set(next);
          return true;
        }
        return false;
      }
    };
  }
}
