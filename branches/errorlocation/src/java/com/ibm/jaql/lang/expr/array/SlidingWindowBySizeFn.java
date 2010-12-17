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
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.array.SlidingWindowFn.BufferedJsonIterator;
import com.ibm.jaql.lang.expr.array.SlidingWindowFn.JsonQueue;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

public class SlidingWindowBySizeFn extends IterExpr
{
  public static final JsonString TAG_CUR = new JsonString("cur");
  public static final JsonString TAG_WINDOW = new JsonString("window");
  protected static final JsonString[] names = new JsonString[] { TAG_CUR, TAG_WINDOW };
  
  public static class Descriptor implements BuiltInFunctionDescriptor 
  {
    private Schema schema = new ArraySchema(null, SchemaFactory.recordSchema()); // TODO: add fields; add instance schema with types
    private JsonValueParameters parameters;

    public Descriptor() {
      parameters = new JsonValueParameters(new JsonValueParameter[] {
          new JsonValueParameter("input", SchemaFactory.arrayOrNullSchema()),
          new JsonValueParameter("size", SchemaFactory.longSchema()),
          new JsonValueParameter("offset", SchemaFactory.longOrNullSchema(), null), // default is 1-size; i.e., a trailing window
          new JsonValueParameter("exact", SchemaFactory.booleanSchema(), JsonBool.FALSE) // only return windows of the exact size, default is to ramp up
      });
    }

    @Override
    public Expr construct(Expr[] positionalArgs)
    {
      return new SlidingWindowBySizeFn(positionalArgs);
    }

    @Override
    public Class<? extends Expr> getImplementingClass() 
    {
      return SlidingWindowBySizeFn.class;
    }

    @Override
    public String getName()
    {
      return "slidingWindowBySize";
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

  public SlidingWindowBySizeFn(Expr... exprs)
  {
    super(exprs);
  }

  public final Expr inputExpr()
  {
    return exprs[0];
  }

  public final Expr sizeExpr()
  {
    return exprs[1];
  }

  public final Expr offsetExpr()
  {
    return exprs[2];
  }

  public final Expr exactExpr()
  {
    return exprs[3];
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.arraySchema(); // TODO: fill in
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    // TODO: the ItemHashtable is a real quick and dirty prototype.  We need to spill to disk, etc...
    final JsonIterator iter = inputExpr().iter(context);
    
    final int size = ((JsonNumber)sizeExpr().eval(context)).intValue();
    JsonNumber joffset = (JsonNumber)offsetExpr().eval(context);
    final long offset = joffset == null ? 1 - size : joffset.longValue();

    final boolean exact = ((JsonBool)exactExpr().eval(context)).get();
    
    // Window size <= 0 exact == true is not handled by the general case
    // below, so we will specially handle the size <= 0 case, which is easy
    // to handle.
    if( size <= 0 )
    {
      if( size < 0 && exact )
      {
        return JsonIterator.EMPTY;
      }
      else // size == 0 || (size < 0 && !exact)
      {
        // Create the output record
        final BufferedJsonRecord result = new BufferedJsonRecord(2);
        JsonValue[] values = new JsonValue[] { null, JsonArray.EMPTY };
        result.set(names, values, names.length, true);

        return new JsonIterator(result) 
        {
          protected boolean moveNextRaw() throws Exception
          {
            if( iter.moveNext() )
            {
              result.set(0, iter.current());
              return true;
            }
            return false;
          }
        };
      }
    }
    // We could also optimize (size=1,offset=0) because no queue is required.
    
    // Initialize the iterators
    final JsonQueue queue = new JsonQueue();
    final BufferedJsonIterator cur   = new BufferedJsonIterator(iter, queue);
    final BufferedJsonIterator start = new BufferedJsonIterator(iter, queue);
    final BufferedJsonIterator next  = new BufferedJsonIterator(iter, queue);

    if( ! cur.moveNext() )
    {
      return JsonIterator.EMPTY;
    }
    start.moveNext();
    next.moveNext();

    // Advance window pointers
    long numNext;
    if( offset > 0 )
    {
      // Advance the start and next pointers to the offset
      start.skipN(offset);
      next.skipTo(start);
      numNext = size;
    }
    else // offset <= 0
    {
      if( exact && offset < 0 )
      {
        long n = -offset;
        if( cur.skipN(n) < n )
        {
          // We can't form even one full window
          return JsonIterator.EMPTY;
        }
        numNext = size;
      }
      else
      {
        numNext = offset + size; 
      }
    }

    if( next.skipN(numNext) < numNext - 1 )
    {
      if( exact )
      {
        // We can't form even one full window
        return JsonIterator.EMPTY;
      }
    }

    // Prune the buffer
    queue.removeTo( Math.min(cur.loc(), start.loc()) );

    // Create the output record
    final SpilledJsonArray window = new SpilledJsonArray();
    final BufferedJsonRecord result = new BufferedJsonRecord(2);
    JsonValue[] values = new JsonValue[] { null, window };
    result.set(names, values, names.length, true);

    return new JsonIterator(result) 
    {
      protected long startDelay = exact ? 0 : -offset;
      protected long nextDelay = exact ? 0 : startDelay - size;
      protected boolean eof = false;
      
      protected boolean moveNextRaw() throws Exception
      {
        if( eof )
        {
          return false;
        }
        
        // Prune the buffer
        queue.removeTo( Math.min(cur.loc(), start.loc()) );

        // Set the result
        result.set(0, cur.current());
        // Copy the window from start to next - 1
        // TODO: make a JsonQueue subset to avoid the copy
        start.toArray(window,next);

        // Advance the current pointer
        eof = ! cur.moveNext();
        
        // Advance the start pointer unless we are still ramping up the window
        if( startDelay > 0 )
        {
          startDelay--;
        }
        else
        {
          start.moveNext();
        }
        
        // Advance the next pointer.
        if( nextDelay > 0 )
        {
          nextDelay--;
        }
        else if( next.atEof() )
        {
          // The window is shrinking.
          if( exact )
          {
            eof = true;
          }
        }
        else 
        {
          next.moveNext();
        }
        
        return true;
      }
    };
  }
}
