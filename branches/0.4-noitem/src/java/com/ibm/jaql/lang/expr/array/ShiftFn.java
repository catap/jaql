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

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JNumber;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.util.Bool3;

@JaqlFn(fnName="shift",minArgs=2,maxArgs=3)
public class ShiftFn extends IterExpr
{

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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
   */
  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Iter iter(final Context context) throws Exception
  {
    // TODO: the ItemHashtable is a real quick and dirty prototype.  We need to spill to disk, etc...
    final Iter iter = exprs[0].iter(context);
    if( iter.isNull() )
    {
      return Iter.nil;
    }
    
    JNumber beforeNum = (JNumber)exprs[1].eval(context).get();
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
    if( exprs.length == 2 )
    {
      after = 0;
    }
    else
    {
      JNumber afterNum = (JNumber)exprs[2].eval(context).get();
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
    }

    if( before + after + 1 > Integer.MAX_VALUE )
    {
      throw new RuntimeException("sorry, we assume the window fits in memory...");
    }

    final int size = (int)(before + after + 1);
    final InMemoryCircularItemBuffer buffer = new InMemoryCircularItemBuffer(size);
    final ResettableIter window = buffer.iter();
    final FixedJArray arr = new FixedJArray(size);
    final Item result = new Item(arr);
    
    Item item = Item.NIL;
    for( long i = 0 ; i < after ; i++ )
    {
      if( item != null ) // Don't call next() once we hit eof
      {
        item = iter.next();
      }
      buffer.add(item);
    }

    final Item tmpItem = item;

    return new Iter() 
    {
      long tail = after;
      Item item = tmpItem; 
      
      public Item next() throws Exception
      {
        if( item != null )
        {
          item = iter.next();
        }
        if( item == null )
        {
          if( tail == 0 )
          {
            return null;
          }
          tail--;
        }
        buffer.add(item);
        window.reset();
        for(int i = 0 ; i < size ; i++)
        {
          arr.set(i, window.next());
        }
        return result;
      }
    };
  }

  
  public static abstract class ResettableIter extends Iter // TODO: move out
  {
    public abstract void reset();
  }
  
  public static class InMemoryCircularItemBuffer // TODO: move out
  {
    private Item[] buffer;
    private int end;
    
    public InMemoryCircularItemBuffer(int size)
    {
      buffer = new Item[size+1];
      for(int i = 0 ; i < buffer.length ; i++)
      {
        buffer[i] = new Item();
      }
      end = size;
    }
    
    public void add(Item item) throws Exception
    {
      buffer[end].setCopy(item);
      end++;
      if( end == buffer.length )
      {
        end = 0;
      }
    }
    
    public ResettableIter iter()
    {
      return new ResettableIter()
      {
        int i = end + 1;
        
        @Override
        public Item next() throws Exception
        {
          if( i == buffer.length )
          {
            i = 0;
          }
          if( i == end )
          {
            return null;
          }
          Item item = buffer[i++];
          return item;
        }

        public void reset()
        {
          i = end + 1;
        }
      };
    }
  }
}
