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

import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;


@JaqlFn(fnName = "columnwise", minArgs = 1, maxArgs = 1)
public class ColumnwiseFn extends Expr
{
  /**
   * exprs
   * 
   * @param exprs
   */
  public ColumnwiseFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr: [ {*}<*> ]?
   */
  public ColumnwiseFn(Expr expr)
  {
    super(expr);
  }

  /**
   * 
   */
  @Override
  public Item eval(final Context context) throws Exception
  {
    Iter iter = exprs[0].iter(context);
    if( iter.isNull() )
    {
      return Item.nil;
    }
    MemoryJRecord rec = new MemoryJRecord();
    Item result = new Item(rec);
    HashMap<JString,SpillJArray> temp = new HashMap<JString, SpillJArray>();
    ArrayList<SpillJArray> arrays = new ArrayList<SpillJArray>();
    int count = 0;
    
    Item item;
    while( (item = iter.next()) != null )
    {
      JRecord r = (JRecord)item.get();
      if( r == null )
      {
        continue;
      }
      int n = r.arity();
      for( int i = 0 ; i < n ; i++ )
      {
        JString name = r.getName(i);
        Item val = r.getValue(i);
        SpillJArray arr = temp.get(name);
        if( arr == null )
        {
          arr = new SpillJArray();
          temp.put(name, arr);
          arrays.add(arr);
          rec.add(name, arr);
          // pad the array with initial nulls
          for(int j = 0 ; j < count ; j++)
          {
            arr.addCopy(Item.nil);
          }
        }
        arr.addCopy(val);
      }
      count++;
      // add null for any column that wasn't in this record
      n = arrays.size();
      for(int i = 0 ; i < n ; i++)
      {
        SpillJArray arr = arrays.get(i);
        if( arr.count() != count )
        {
          arr.addCopy(Item.nil);
        }
      }
    }
    
    return result;
  }
}
