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
package com.ibm.jaql.lang.expr.index;

import java.util.HashMap;

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * [ [key,value1] ] -> keyLookup([ [key,value2] ]) ==> [ [key, value1, value2] ]
 * 
 * Build a hash table on the inner key/value pairs (expr[1]).
 * For each key/value in the outer pairs (expr[0])
 *   return [key, value1, value2] tuples.
 *   
 * The code assumes that the inner keys are unique (or an arbitrary value is kept)
 *    //TODO: support duplicates?  raise error?
 * 
 * If the outer key does not exist in the inner set, 
 *   null is returned for the inner value.
 *   So this is preserving the outer input (left outer join)
 *   // TODO: support full outer by finding inner values that didn't join?
 * 
 *   // TODO:support spilling large inners? 
 * 
 * @author kbeyer
 */
@JaqlFn(fnName = "keyLookup", minArgs = 2, maxArgs = 2)
public class KeyLookupFn extends IterExpr
{
  public KeyLookupFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * This expression can be applied in parallel per partition of child i.
   */
  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
  }

  @Override
  public Iter iter(final Context context) throws Exception
  {
    final HashMap<Item,Item> inner = new HashMap<Item,Item>();
    final Item[] keyval = new Item[2];
    Iter iter = exprs[1].iter(context);
    Item item;
    while( (item = iter.next()) != null )
    {
      JArray a = (JArray)item.getNonNull();
      a.getTuple(keyval);
      Item key = new Item();
      Item val = new Item();
      key.setCopy(keyval[0]);
      val.setCopy(keyval[1]);
      inner.put(key, val);
    }
    
    return new Iter()
    {
      FixedJArray resultArray = new FixedJArray(3);
      Item result = new Item(resultArray);
      Iter iter = exprs[0].iter(context);
      
      @Override
      public Item next() throws Exception
      {
        Item item = iter.next();
        if( item == null )
        {
          return null;
        }
        JArray a = (JArray)item.getNonNull();
        a.getTuple(keyval);
        Item val = inner.get(keyval[0]);
        if( val == null )
        {
          val = Item.NIL;
        }
        resultArray.set(0, keyval[0]);
        resultArray.set(1, keyval[1]);
        resultArray.set(2, val);
        return result;
      }
    };
  }
}
