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
package com.ibm.jaql.lang.expr.array;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * powerset<T>([T...]? arr) returns [ [T...]... ] 
 * Return the power-set (really the power-list) of a list of items.
 *     
 * If arr has k items, the result has k - 1 items.
 * result[].prev is the first k-1 items
 * result[].cur  is the last k-1 items.
 *
 * eg: [1,2,3] -> powerset() ==  [ [], [1], [2], [1,2], [3], [1,3], [2,3], [1,2,3] ] 
 */
public class PowersetFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("powerset", PowersetFn.class);
    }
  }
  
  public PowersetFn(Expr[] inputs)
  {
    super(inputs);
  }

  
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  @Override
  public Schema getSchema()
  {
    Schema s = exprs[0].getSchema();
    // TODO: Add JsonUtil.requireType(JsonType, JsonType...) 
    if( s == null || s.is(JsonType.ARRAY, JsonType.NULL).never() )
    {
      throw new RuntimeException("array or null required");
    }
    s = s.elements(); 
    if( s == null )
    {
      // powerset([]) = [[]]
      return new ArraySchema(new Schema[]{SchemaFactory.emptyArraySchema()});
    }
    s = new ArraySchema(null, s);
    return new ArraySchema(new Schema[]{s}, s);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonArray input1 = (JsonArray)exprs[0].eval(context);
    if( input1 == null )
    {
      input1 = JsonArray.EMPTY;  // TODO: return null?
    }
    final JsonArray input = input1;
    if( input.count() > 30 )
    {
      throw new RuntimeException("powerset results over 2^30 are not supported");
    }
    final int powerSize = 1 << (int)input.count();
    final SpilledJsonArray result = new SpilledJsonArray();  
    return new JsonIterator(result) 
    {
      int selected = 0;
      
      public boolean moveNext() throws Exception
      {
        if( selected >= powerSize )
        {
          return false;
        }
        // TODO: this could be made faster
        result.clear();
        for( int j = 0, k = selected ; k > 0 ; j++, k >>>= 1 ) 
        {
          if( (k & 0x01) == 1 )
          {
            result.add( input.get(j) );
          }
        }
        selected++;
        return true;
      }
    };
  }
}
