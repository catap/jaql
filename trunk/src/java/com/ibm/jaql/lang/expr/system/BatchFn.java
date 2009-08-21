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
package com.ibm.jaql.lang.expr.system;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JaqlFunction;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.util.Bool3;

/**
 * batch( arr, n, fn ) 
 * invokes fn
 *
 */
@JaqlFn(fnName = "batch", minArgs = 3, maxArgs = 3)
public class BatchFn extends IterExpr
{
  public BatchFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    // This could report true for the function expression, but it
    // does evaluate the function multiple times.
    return (i == 0) ? Bool3.TRUE : Bool3.UNKNOWN;
  }

  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
  }

  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonNumber jnum = (JsonNumber)exprs[1].eval(context);
    long tbatchSize = jnum.longValue();
    if( tbatchSize < 1 )
    {
      tbatchSize = SpilledJsonArray.DEFAULT_CACHE_SIZE;
    }
    int tbufferSize = SpilledJsonArray.DEFAULT_CACHE_SIZE * 10; // TODO: tune
    if( tbatchSize < tbufferSize )
    {
      tbufferSize = (int)tbatchSize;
    }
    
    final long batchSize = tbatchSize;
    final int bufferSize = tbufferSize;
    final BufferedJsonArray pair = new BufferedJsonArray(2);
    
    return new JsonIterator(pair)
    {
      SpilledJsonArray batch = new SpilledJsonArray(bufferSize);
      JsonIterator iter = exprs[0].iter(context);
      JaqlFunction fn = (JaqlFunction)exprs[2].eval(context);
      JsonIterator batchIter = JsonIterator.EMPTY;
      JsonIterator fnIter = JsonIterator.EMPTY;

      @Override
      public boolean moveNext() throws Exception
      {
        while( true )
        {
          // Return the next pair from the batch/function, if available
          if( batchIter.moveNext() )
          {
            if( !fnIter.moveNext() )
            {
              throw new RuntimeException("function must return exactly one item per batch item.\n"+
                  " Batch at:"+batchIter.current());
            }
            pair.set(0, batchIter.current());
            pair.set(1, fnIter.current());
            return true;
          }
          else if( fnIter.moveNext() )
          {
            throw new RuntimeException("function must return exactly one item per batch item.\n"+
                " Function at:"+fnIter.current());
          }

          // load a batch
          batch.clear();
          for( long i = 0 ; i < batchSize ; i++ )
          {
            if( ! iter.moveNext() )
            {
              if( i == 0 )
              {
                return false;
              }
              break;
            }
            JsonValue value = iter.current();
            batch.addCopy(value);
          }
          
          // invoke the function on the batch
          batchIter = batch.iter();
          fnIter = fn.iter(context, batch);
        }
      }
    };
  }

}
