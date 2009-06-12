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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;


@JaqlFn(fnName = "rowwise", minArgs = 1, maxArgs = 1)
public class RowwiseFn extends IterExpr
{
  /**
   * exprs
   * 
   * @param exprs
   */
  public RowwiseFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr: { *:[*<*>] }?
   */
  public RowwiseFn(Expr expr)
  {
    super(expr);
  }

  /**
   * 
   */
  @Override
  public Iter iter(final Context context) throws Exception
  {
    Item item = exprs[0].eval(context);
    if( item.isNull() )
    {
      return Iter.nil;
    }
    JRecord inrec = (JRecord)item.get();
    int n = inrec.arity();
    final ArrayList<JString> names = new ArrayList<JString>(n);
    final ArrayList<Iter> values = new ArrayList<Iter>(n);
    for(int i = 0 ; i < n ; i++)
    {
      JArray val = (JArray)inrec.getValue(i).get();
      if( val != null && ! val.isEmpty() )
      {
        names.add(inrec.getName(i));
        values.add(val.iter());
      }
    }
    if( names.isEmpty() )
    {
      return Iter.empty;
    }
    final MemoryJRecord outrec = new MemoryJRecord();
    final Item result = new Item(outrec);
    
    return new Iter()
    {      
      @Override
      public Item next() throws Exception
      {
        outrec.clear();
        int n = names.size();
        int done = 0;
        for(int i = 0 ; i < n ; i++)
        {
          JString name = names.get(i);
          Iter iter = values.get(i);
          Item item = iter.next();
          if( item == null )
          {
            values.set(i, Iter.empty);
            done++;
          }
          else if( ! item.isNull() )
          {
            outrec.add(name, item);
          }
        }
        if( done == n )
        {
          return null;
        }
        return result;
      }
    };
  }
}
