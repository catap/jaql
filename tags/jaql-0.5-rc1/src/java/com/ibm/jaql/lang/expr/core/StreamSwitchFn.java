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
package com.ibm.jaql.lang.expr.core;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.util.Bool3;

/**
 * e0 -> streamSwitch( f0, ..., fn )
 * ===
 * ( x = e0,
 *   union( x -> filter $[0] == 0 -> transform $[1] -> f0(),
 *          ...
 *          x -> filter $[0] == n -> transform $[1] -> fn() ) 
 * )
 * 
 * Except that the functions can be called any number of times and in any order.
 * Something like this:
 * 
 * ( x = e0,
 *   union( x -> filter $[0] == 0 -> transform $[1] -> batch(n=?) -> expand f0($),
 *          ...
 *          x -> filter $[0] == n -> transform $[1] -> batch(n=?) -> expand fn($) ) 
 * )
 * 
 * The actual implementation is to stream into function fi any consecutive rows
 * with index i.  Something like this:
 * 
 * ( x = e0,
 *   x -> tumblingWindow( stop = fn(first,next) first[0] != next[0] )
 *     -> expand each p fi( p -> transform $[1] ) // where i is p[j][0] for all j in the window
 * )
 * 
 */
public class StreamSwitchFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par2u
  {
    public Descriptor()
    {
      super("streamSwitch", StreamSwitchFn.class);
    }
  }
  
  public StreamSwitchFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.valueOf(i == 0);
  }

  // TODO: schema is union of fi result schemata
//  @Override
//  public Schema getSchema()
//  {
//  }

  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    final Function fns[] = new Function[exprs.length - 1];
    for(int i = 1 ; i < exprs.length ; i++)
    {
      fns[i-1] = (Function)exprs[i].eval(context);
    }
    
    final GroupIterator groupIter = new GroupIterator(exprs[0].iter(context));
    
    return new JsonIterator()
    {
      JsonIterator inner = JsonIterator.EMPTY;
      
      @Override
      public boolean moveNext() throws Exception
      {
        while( true )
        {
          if( inner.moveNext() )
          {
            currentValue = inner.current();
            return true;
          }
        
          if( ! groupIter.nextGroup() )
          {
            inner = JsonIterator.EMPTY; // just to be safe
            return false;
          }
          
          int i = groupIter.getIndex();
          Function fn = fns[i];
          fn.setArguments(groupIter);
          inner = fn.iter(context);
        }
      }
    };
  }
  
  static class GroupIterator extends JsonIterator
  {
    static enum State { GROUP_START, IN_GROUP, GROUP_END, EOF };
    
    protected State state; 
    protected JsonIterator inputIter;
    protected int index = 0;
    protected final JsonValue[] pair = new JsonValue[2];

    
    public GroupIterator(JsonIterator inputIter) throws Exception
    {
      this.inputIter = inputIter;
      if( inputIter.moveNext() )
      {
        index = getCurrentPair();
        state = State.GROUP_END;
      }
      else
      {
        state = State.EOF;
      }
    }
    
    protected int getCurrentPair() throws Exception
    {
      JsonArray jarray = (JsonArray)inputIter.current();
      jarray.getAll(pair);
      currentValue = pair[1];
      JsonNumber jnum = (JsonNumber)pair[0];
      return jnum.intValueExact();
    }
    
    public boolean nextGroup() throws Exception
    {
      if( state == State.EOF )
      {
        return false;
      }
      assert state == State.GROUP_END;
      state = State.GROUP_START;
      return true;
    }
    
    public int getIndex()
    {
      return index;
    }
    
    @Override
    public boolean moveNext() throws Exception
    {
      if( state == State.IN_GROUP )
      {
        if( ! inputIter.moveNext() )
        {
          state = State.EOF;
          return false;
        }
        
        int i = getCurrentPair();
        if( i == index )
        {
          currentValue = pair[1];
          return true;
        }

        index = i;
        state = State.GROUP_END;
        return false;
      }
      else if( state == State.GROUP_START )
      {
        state = State.IN_GROUP;
        return true;
      }
      else
      {
        return false;
      }
      
    }
  }

}
