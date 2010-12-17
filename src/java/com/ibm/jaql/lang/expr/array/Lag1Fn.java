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
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
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
 * eg: [1,2,3] -> lag1()  ==  [ { prev: 1, cur: 2 }, { prev: 2, cur: 3 } ] 
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
  
  public final static JsonString CUR  = new JsonString("cur");
  public final static JsonString PREV = new JsonString("prev");
  
  private final Schema schema;
  
  public Lag1Fn(Expr[] inputs)
  {
    super(inputs);
    Schema inSchema = exprs[0].getSchema().elements();
    if( inSchema == null )
    {
      schema = null;
    }
    else
    {
      Schema itemSchema = new RecordSchema(new RecordSchema.Field[]{
          new RecordSchema.Field(CUR, inSchema, false),
          new RecordSchema.Field(PREV, inSchema, false)
      }, null);
      schema = new ArraySchema(null,itemSchema);
    }
  }

  @Override
  public Schema getSchema()
  {
    return schema;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    final JsonIterator iter = exprs[0].iter(context);
    if( iter.isNull() )
    {
      return JsonIterator.NULL;
    }
    
    final BufferedJsonRecord rec = new BufferedJsonRecord(2);
    final JsonString[] names = new JsonString[] { CUR, PREV };
    final JsonValue[] values = new JsonValue[2];
    rec.set(names, values, 2, true);
    
    if( ! iter.moveNext() )
    {
      return JsonIterator.EMPTY;
    }
    values[0] = iter.current();

    return new JsonIterator(rec) 
    {
      protected boolean moveNextRaw() throws Exception
      {
        values[1] = JsonUtil.getCopy(values[0], values[1]); // prev = cur
        if( ! iter.moveNext() )
        {
          return false;
        }
        values[0] = iter.current(); // cur = iter.next()
        return true;
      }
    };
  }
}
