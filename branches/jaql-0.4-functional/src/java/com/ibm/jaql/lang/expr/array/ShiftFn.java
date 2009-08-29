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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

public class ShiftFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par23
  {
    public Descriptor()
    {
      super("shift", ShiftFn.class);
    }
  }
  
  // input expr, num before, (num after)?
  public ShiftFn(Expr[] inputs)
  {
    super(inputs);
  }

  /**
   * @return
   */
  public final Expr beforeExpr()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final Expr afterExpr()
  {
    return exprs[1];
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.arraySchema();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    // TODO: the ItemHashtable is a real quick and dirty prototype.  We need to spill to disk, etc...
    final JsonIterator iter = exprs[0].iter(context);
    if( iter.isNull() )
    {
      return JsonIterator.NULL;
    }
    
    JsonNumber beforeNum = (JsonNumber)exprs[1].eval(context);
    final long before;
    if( beforeNum == null )
    {
      before = 0;
    }
    else
    {
      before = beforeNum.longValueExact();
      if( before < 0 )
      {
        throw new RuntimeException("shift before must be >= 0 got "+before);
      }
    }

    final long after;
    JsonNumber afterNum = (JsonNumber)exprs[2].eval(context);
    if( afterNum == null )
    {
      after = 0;
    }
    else
    {
      after = afterNum.longValueExact();
      if( after < 0 )
      {
        throw new RuntimeException("shift after must be >= 0 got "+after);
      }
    }

    if( before + after + 1 > Integer.MAX_VALUE )
    {
      throw new RuntimeException("sorry, we assume the window fits in memory...");
    }

    final int size = (int)(before + after + 1);
    final InMemoryCircularJsonBuffer buffer = new InMemoryCircularJsonBuffer(size);
    final ResettableJsonIterator window = buffer.iter();
    final BufferedJsonArray arr = new BufferedJsonArray(size);
    
    boolean eof = false;
    JsonValue value = null;
    for( long i = 0 ; i < after ; i++ )
    {
      if (!eof && iter.moveNext()) { // Don't call moveNext() once we hit eof
        value = iter.current();
        eof = true;
      } else {
        value = null;
      }
      buffer.add(value);
    }

    return new JsonIterator(arr) 
    {
      boolean eof = false;
      long tail = after;
      
      public boolean moveNext() throws Exception
      {
        if (!eof) {
          eof = iter.moveNext();
        }
        if( eof )
        {
          if( tail == 0 )
          {
            return false;
          }
          tail--;
          buffer.add(null);
        } 
        else 
        {
          buffer.add(iter.current());
        }
        window.reset();
        for(int i = 0 ; i < size ; i++)
        {
          boolean hasNext = window.moveNext();
          assert hasNext;
          arr.set(i, window.current());
        }
        return true; // currentValue == arr
      }
    };
  }

  
  public static abstract class ResettableJsonIterator extends JsonIterator // TODO: move out
  {
    public abstract void reset();
  }
  
  public static class InMemoryCircularJsonBuffer // TODO: move out
  {
    private JsonValue[] buffer;
    private int end;
    
    public InMemoryCircularJsonBuffer(int size)
    {
      buffer = new JsonValue[size+1];
      end = size;
    }
    
    public void add(JsonValue value) throws Exception
    {
      if (value == null) 
      {
        buffer[end] = null;
      }
      else
      {
        buffer[end] = value.getCopy(buffer[end]);
      }
      end++;
      if( end == buffer.length )
      {
        end = 0;
      }
    }
    
    public ResettableJsonIterator iter()
    {
      return new ResettableJsonIterator()
      {
        int i = end + 1;
        
        @Override
        public boolean moveNext() throws Exception
        {
          if( i == buffer.length )
          {
            i = 0;
          }
          if( i == end )
          {
            return false;
          }
          currentValue = buffer[i++];
          return true;
        }

        public void reset()
        {
          i = end + 1;
        }
      };
    }
  }
}
