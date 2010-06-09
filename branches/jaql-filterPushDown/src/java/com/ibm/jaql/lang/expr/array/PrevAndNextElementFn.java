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
 * arr -> prevAndNextElement() 
 *    arr is [ T ], 
 *    returns [ { cur: T, prev?: T, next?: T } ]
 *     
 * If arr has k items, the result has k items.
 * The first record returned does not have a prev field.
 * The last record returned does not have a next field.
 *
 * eg: [1,2,3] -> prevAndNextElement() ==  
 *  [ { cur: 1, next: 2,  }, 
 *    { cur: 2, prev: 1, next: 3 }, 
 *    { cur: 3, prev: 2 } ] 
 */
public class PrevAndNextElementFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("prevAndNextElement", PrevAndNextElementFn.class);
    }
  }
  
  public final static JsonString CUR  = new JsonString("cur");
  public final static JsonString PREV = new JsonString("prev");
  public final static JsonString NEXT = new JsonString("next");

  private final Schema schema;
  
  public PrevAndNextElementFn(Expr... inputs)
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
          new RecordSchema.Field(PREV, inSchema, true),
          new RecordSchema.Field(NEXT, inSchema, true)
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
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonIterator iter = exprs[0].iter(context);
    
    if( ! iter.moveNext() )
    {
      return JsonIterator.EMPTY;
    }
    
    final BufferedJsonRecord rec = new BufferedJsonRecord(0);
    
    return new JsonIterator(rec) 
    {
      JsonValue cur = null;
      JsonValue prev = null;
      JsonValue next = iter.current();
      boolean hasPrev = false;
      boolean hasNext = true;

      final JsonString[] names = new JsonString[] { CUR, PREV, NEXT };
      final JsonValue[] values = new JsonValue[3];
      
      public boolean moveNext() throws Exception
      {
        if( !hasNext )
        {
          return false;
        }
        // TODO: Avoid a copy by swapping buffers
        prev = JsonUtil.getCopy(cur, prev);
        cur = JsonUtil.getCopy(next, cur);
        if( (hasNext = iter.moveNext()) )
        {
          next = iter.current();
        }
        
        // names[0] = CUR;
        values[0] = cur;
        int n = 1;
        if( hasPrev )
        {
          names[n] = PREV;
          values[n++] = prev;
        }
        if( hasNext )
        {
          names[n] = NEXT;
          values[n++] = next; 
        }
        rec.set(names, values, n, false);
        hasPrev = true;
        return true;
      }
    };
  }
}
