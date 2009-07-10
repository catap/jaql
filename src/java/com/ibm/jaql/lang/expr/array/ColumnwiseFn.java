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
import java.util.Map.Entry;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
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
  public JsonValue eval(final Context context) throws Exception
  {
    JsonIterator iter = exprs[0].iter(context);
    if( iter.isNull() )
    {
      return null;
    }
    BufferedJsonRecord rec = new BufferedJsonRecord();
    HashMap<JsonString,SpilledJsonArray> temp = new HashMap<JsonString, SpilledJsonArray>();
    ArrayList<SpilledJsonArray> arrays = new ArrayList<SpilledJsonArray>();
    int count = 0;
    
    for (JsonValue value : iter)
    {
      JsonRecord r = (JsonRecord)value;
      if( r == null )
      {
        continue;
      }
      for (Entry<JsonString, JsonValue> e : r)
      {
        JsonString name = e.getKey();
        JsonValue val = e.getValue();
        SpilledJsonArray arr = temp.get(name);
        if( arr == null )
        {
          arr = new SpilledJsonArray();
          temp.put(name, arr);
          arrays.add(arr);
          rec.add(name, arr);
          // pad the array with initial nulls
          for(int j = 0 ; j < count ; j++)
          {
            arr.addCopy(null);
          }
        }
        arr.addCopy(val);
      }
      count++;
      // add null for any column that wasn't in this record
      int n = arrays.size();
      for(int i = 0 ; i < n ; i++)
      {
        SpilledJsonArray arr = arrays.get(i);
        if( arr.count() != count )
        {
          arr.addCopy(null);
        }
      }
    }
    
    return rec;
  }
}
