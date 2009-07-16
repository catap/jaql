/*
 * Copyright (C) IBM Corp. 2008.
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

import java.util.ArrayList;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.util.Bool3;

/**
 * Merge a set of arrays into one array in order, or a set of records into one record.  Nulls are ignored.
 * 
 * @author kbeyer
 *
 */
@JaqlFn(fnName="mergeContainers", minArgs=1, maxArgs=Expr.UNLIMITED_EXPRS)
public class MergeContainersFn extends Expr
{
  public MergeContainersFn(Expr[] inputs)
  {
    super(inputs);
  }
  
  public MergeContainersFn(ArrayList<Expr> inputs)
  {
    super(inputs);
  }

  public MergeContainersFn(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }

  /**
   * This is an array if all inputs produce an array.
   */
  @Override
  public Bool3 isArray()
  {
    Bool3 rc = Bool3.TRUE;
    for(Expr e: exprs)
    {
      rc = rc.and( e.isArray() );
    }
    return rc;
  }

  /**
   * This is empty if all inputs produce are empty.
   */
  @Override
  public Bool3 isEmpty()
  {
    Bool3 rc = Bool3.TRUE;
    for(Expr e: exprs)
    {
      rc = rc.and( e.isEmpty() );
    }
    return rc;
  }

  /**
   * This is null if all inputs produce null.
   */
  @Override
  public Bool3 isNull()
  {
    Bool3 rc = Bool3.TRUE;
    for(Expr e: exprs)
    {
      rc = rc.and( e.isNull() );
    }
    return rc;
  }
  
  @Override
  public Item eval(Context context) throws Exception
  {
    int i;
    Item item = null;
    for( i = 0 ; i < exprs.length ; i++ )
    {
      item = exprs[i].eval(context);
      if( ! item.isNull() )
      {
        break;
      }
    }
    JValue val = item.get();
    Item result;
    if( val == null )
    {
      result = Item.nil;
    }
    else if( val instanceof JArray )
    {
      SpillJArray resultArr = new SpillJArray();
      while(true)
      {
        if( val != null )
        {
          JArray arr = (JArray)val;
          resultArr.addAll(arr.iter());
        }
        i++;
        if( i >= exprs.length )
        {
          break;
        }
        val = exprs[i].eval(context).get();
      }
      result = new Item(resultArr); // TODO: memory
    }
    else if( val instanceof JRecord )
    {
      MemoryJRecord resultRec = new MemoryJRecord(); // TODO: memory
      while(true)
      {
        if( val != null )
        {
          JRecord rec = (JRecord)val;
          for(int j = 0 ; j < rec.arity() ; j++)
          {
            resultRec.add(rec.getName(j), rec.getValue(j));
          }
        }
        i++;
        if( i >= exprs.length )
        {
          break;
        }
        val = exprs[i].eval(context).get();
      }
      result = new Item(resultRec); // TODO: memory
    }
    else
    {
      throw new RuntimeException("mergeContainers() can only merge all arrays or all records");
    }
    
    return result;
  }

  @Override
  public Iter iter(final Context context) throws Exception
  {
    return new Iter()
    {
      int input = 0;
      Iter iter = Iter.empty;
      
      @Override
      public Item next() throws Exception
      {
        while( true )
        {
          Item item = iter.next();
          if( item != null )
          {
            return item;
          }
          if( input >= exprs.length )
          {
            return null;
          }
          iter = exprs[input++].iter(context);
        }
      }
    };
  }
}
