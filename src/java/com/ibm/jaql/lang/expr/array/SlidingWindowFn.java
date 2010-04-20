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

import java.io.IOException;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 *  
 *
 */
public class SlidingWindowFn extends IterExpr
{
  public static final JsonString TAG_CUR = new JsonString("cur");
  public static final JsonString TAG_WINDOW = new JsonString("window");
  protected static final JsonString[] names = new JsonString[] { TAG_CUR, TAG_WINDOW };
  
  protected static final int ARG_INPUT = 0;
  protected static final int ARG_START = 1;
  protected static final int ARG_END   = 2;
  
  public static class Descriptor implements BuiltInFunctionDescriptor 
  {
    private Schema schema = new ArraySchema(null, SchemaFactory.recordSchema()); // TODO: add fields; add instance schema with types
    private JsonValueParameters parameters;

    public Descriptor() {
      parameters = new JsonValueParameters(new JsonValueParameter[] {
          new JsonValueParameter("input", SchemaFactory.arrayOrNullSchema()),
          new JsonValueParameter("start", SchemaFactory.functionSchema()),
          new JsonValueParameter("end", SchemaFactory.functionSchema())
      });
    }

    @Override
    public Expr construct(Expr[] positionalArgs)
    {
      return new SlidingWindowFn(positionalArgs);
    }

    @Override
    public Class<? extends Expr> getImplementingClass() 
    {
      return SlidingWindowFn.class;
    }

    @Override
    public String getName()
    {
      return "slidingWindow";
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

  
  public SlidingWindowFn(Expr... inputs)
  {
    super(inputs);
  }

  @Override
  public Schema getSchema()
  {
    Schema T = exprs[ARG_INPUT].getSchema().elements();
    if( T == null )
    {
      return null;
    }
    Schema window = new ArraySchema(null, T);
    Field[] fields = new Field[] {
        new Field(names[0], T, false),
        new Field(names[1], window, false)
    };
    Schema rec = new RecordSchema(fields , null);
    return new ArraySchema(null, rec);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonIterator iter = exprs[ARG_INPUT].iter(context);
    final Function startPred = (Function)exprs[ARG_START].eval(context);
    final Function endPred = (Function)exprs[ARG_END].eval(context);

    if( ! startPred.canBeCalledWith(2) )
    {
      throw new RuntimeException("start predicate must be fn(first,cur)");
    }
    
    if( ! endPred.canBeCalledWith(2) )
    {
      throw new RuntimeException("end predicate must be fn(cur,last)");
    }

    if( ! iter.moveNext() )
    {
      return JsonIterator.EMPTY;
    }
    
    // Initialize the iterators
    final JsonQueue queue = new JsonQueue();
    queue.addCopy(iter.current());
    final BufferedJsonIterator cur = new BufferedJsonIterator(iter, queue);
    final BufferedJsonIterator start = new BufferedJsonIterator(iter, queue);
    final BufferedJsonIterator next = new BufferedJsonIterator(iter, queue);
    start.moveNext();

    // Create the output record
    final SpilledJsonArray window = new SpilledJsonArray();
    final BufferedJsonRecord result = new BufferedJsonRecord(2);
    JsonValue[] values = new JsonValue[] { null, window };
    result.set(names, values, names.length, true);

    return new JsonIterator(result) 
    {
      final JsonValue[] args = new JsonValue[2];

      public boolean moveNext() throws Exception
      {
        // Advance the current pointer
        if( ! cur.moveNext() )
        {
          return false;
        }
        result.set(0, cur.current());
        
        // Advance the start pointer to the first value at or after
        // the previous start pointer that satisfies the startPred.
        args[1] = cur.current();
        do
        {
          args[0] = start.current();
          startPred.setArguments(args);
        } while( ! JaqlUtil.ebv(startPred.eval(context)) &&
                 start.moveNext() );

        // Advance the next pointer at least the new start pointer.
        next.skipTo(start);

        // Prune the buffer
        queue.removeTo( Math.min(cur.loc(), start.loc()) );

        // Advance the next pointer to the first value at or after both
        // the new start pointer and and the previous next pointer
        // that does not satisify the endPred.
        args[0] = cur.current();
        do
        {
          args[1] = next.current();
          endPred.setArguments(args);
        } while( JaqlUtil.ebv(endPred.eval(context)) &&
                 next.moveNext() );

        
        // Copy the window from start to next - 1
        // TODO: make a JsonQueue subset to avoid the copy
        // TODO: be sure to change add() to addCopy if the Queue starts spilling.
        start.toArray(window,next);
        
        return true;
      }
    };
  }
  
  public static final class JsonQueue  // TODO: move out, make spilling
  {
    public static class Item
    {
      long id;
      Item next;
      JsonValue value;
    }
    
    protected Item head = new Item();
    protected Item tail = head;
    protected Item free;
    protected long nextId = 1;

    
    public boolean isEmpty()
    {
      return head == null;
    }
    
    public Item addCopy(JsonValue value) throws Exception
    {
      Item item;
      if( free == null )
      {
        item = new Item();
      }
      else
      {
        item = free;
        free = item.next;
        item.next = null;
      }
      item.id = nextId++;
      item.value = JsonUtil.getCopy(value, item.value);
      tail = tail.next = item;
      return item;
    }

    public Item head()
    {
      return head;
    } 

    public void removeTo(long id)
    {
      while( head != null && head.id < id )
      {
        Item next = head.next;
        head.next = free;
        free = head;
        head = next;
      }
    }
  }
  
  static final class BufferedJsonIterator extends JsonIterator
  {
    protected JsonIterator iter;
    protected JsonQueue queue;
    protected JsonQueue.Item item;
    
    public BufferedJsonIterator(JsonIterator iter, JsonQueue queue, JsonQueue.Item start)
    {
      this.iter = iter;
      this.queue = queue;
      this.item = start;
    }

    public BufferedJsonIterator(JsonIterator iter, JsonQueue queue)
    {
      this(iter, queue, queue.head());
    }

    public long loc()
    {
      return item == null ? Long.MAX_VALUE : item.id;
    }

    public void toArray(SpilledJsonArray window, BufferedJsonIterator next) throws IOException
    {
      // TODO: be sure to change add() to addCopy if the Queue starts spilling.
      window.clear();
      JsonQueue.Item end = next.item;
      for(JsonQueue.Item i = item ; i != end ; i = i.next )
      {
        window.add(i.value);
      }
    }
    
    /** Return true if the last moveNext() returned false */
    public boolean atEof()
    {
      return item == null;
    }

    @Override
    public boolean moveNext() throws Exception
    {
      if( item == null )
      {
        return false;
      }
      if( item.next == null )
      {
        if( !iter.moveNext() )
        {
          item = null;
          return false;
        }
        queue.addCopy( iter.current() );
      }
      item = item.next;
      currentValue = item.value;
      return true;
    }

    public void skipTo(BufferedJsonIterator min)
    {
      if( item != null  )
      {
        if( min.item == null )
        {
          item = null;
          currentValue = null;
        }
        else if( item.id < min.item.id )
        {
          item = min.item;
          currentValue = item.value;
        }
      }
    }

    public long skipN(long N) throws Exception
    {
      // TODO: this could be made faster
      long i;
      for( i = 0 ; i < N && moveNext() ; i++ )
      {
        // just moving
      }
      return i;
    }
  }
}
