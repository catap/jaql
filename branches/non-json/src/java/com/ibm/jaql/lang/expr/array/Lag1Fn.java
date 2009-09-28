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

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * lag1(arr) 
 *    arr is [ A ], 
 *    returns [ {prev: A, cur: A} ]
 *     
 * If arr has k items, the result has k - 1 items.
 * result[].prev is the first k-1 items
 * result[].cur  is the last k-1 items.
 *
 * eg: [1,2,3] -> lag1()  ==  [ {prev: 1, cur: 2}, { prev: 2, cur: 3 } ] 
 */
public class Lag1Fn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("lag1", Lag1Fn.class);
    }
  }
  
  public Lag1Fn(Expr[] inputs)
  {
    super(inputs);
  }

//  @Override
//  public Schema getSchema()
//  {
//    return SchemaFactory.arraySchema();
//  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonIterator iter = exprs[0].iter(context);
    if( iter.isNull() )
    {
      return JsonIterator.NULL;
    }
    
    final BufferedJsonRecord rec = new BufferedJsonRecord(2);
    final JsonString[] names = new JsonString[] { new JsonString("cur"), new JsonString("prev") };
    final JsonValue[] values = new JsonValue[2];
    rec.set(names, values, 2, true);
    
    if( ! iter.moveNext() )
    {
      return JsonIterator.EMPTY;
    }

    return new JsonIterator(rec) 
    {
      JsonValue prev = null;
      JsonValue cur = iter.current();
      
      public boolean moveNext() throws Exception
      {
        values[1] = prev = cur.getCopy(prev);
        if( ! iter.moveNext() )
        {
          return false;
        }
        values[0] = cur = iter.current();
        return true;
      }
    };
  }
}
