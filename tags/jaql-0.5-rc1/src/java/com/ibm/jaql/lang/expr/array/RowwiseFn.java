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
import java.util.Map.Entry;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;


public class RowwiseFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("rowwise", RowwiseFn.class);
    }
  }
  
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
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonValue value = exprs[0].eval(context);
    if( value == null )
    {
      return JsonIterator.NULL;
    }
    JsonRecord inrec = (JsonRecord)value;
    int n = inrec.size();
    final ArrayList<JsonString> names = new ArrayList<JsonString>(n);
    final ArrayList<JsonIterator> values = new ArrayList<JsonIterator>(n);
    for (Entry<JsonString, JsonValue> e : inrec)
    {
      JsonString name = e.getKey();
      JsonArray val = (JsonArray)e.getValue();
      if( val != null && ! val.isEmpty() )
      {
        names.add(name);
        values.add(val.iter());
      }
    }
    if( names.isEmpty() )
    {
      return JsonIterator.EMPTY;
    }
    final BufferedJsonRecord outrec = new BufferedJsonRecord();
    return new JsonIterator(outrec)
    {      
      @Override
      public boolean moveNext() throws Exception
      {
        outrec.clear();
        int n = names.size();
        int done = 0;
        for(int i = 0 ; i < n ; i++)
        {
          JsonString name = names.get(i);
          JsonIterator iter = values.get(i);
          if (!iter.moveNext())
          {
            values.set(i, JsonIterator.EMPTY);
            done++;
          }
          else if( iter.current() != null  )
          {
            outrec.add(name, iter.current());
          }
        }
        if( done == n )
        {
          return false;
        }
        return true; // currentValue == outRec
      }
    };
  }
}
