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
package com.ibm.jaql.lang.expr.path;

import java.io.InvalidClassException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.SingleJsonValueIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.util.FastPrinter;

public final class UnrollExpr extends IterExpr
{

  /**
   * @param exprs
   */
  public UnrollExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   * @param exprs Expr, ExpandStep+
   */
  public UnrollExpr(ArrayList<Expr> exprs)
  {
    super(exprs);
  }

  
  @Override
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print(kw("unroll") + " ");
    for(int i = 0 ; i < exprs.length ; i++)
    {
      exprs[i].decompile(exprText, capturedVars);
    }
  }
  

  public final JsonIterator iter(final Context context) throws Exception
  {
    // Share as much of input item as possible
    // but copy the expanded part so we can modify it.
    final JsonValue value = exprs[0].eval(context);
    if( value == null )
    {
      return JsonIterator.NULL;
    }
    final JsonValue[] path = new JsonValue[exprs.length]; // TODO: memory
    path[0] = value;
    for(int i = 1 ; i < exprs.length ; i++)
    {
      path[i] = ((UnrollStep)exprs[i]).eval(context, path[i-1]);
      if( path[i] == null ) // input item doesn't have our path
      {
        return new SingleJsonValueIterator(value);
      }
    }

    JsonArray array = (JsonArray)path[exprs.length - 1];
    // array is not null because of check inside the loop.
    
    if( array.isEmpty() )
    {
      // Return item with null value
      JsonValue result = copyTree(value, path, 0, null);
      return new SingleJsonValueIterator(result);
    }

    
    final JsonIterator iter = (array != null) ? array.iter() : JsonIterator.EMPTY;
    
    return new JsonIterator()
    {
      @Override
      public boolean moveNext() throws Exception // TODO: somethign seems wrong here: lastHole is updated but not used
      {
        while( true )
        {
          if (!iter.moveNext()) 
          {
            return false;
          }
          currentValue = copyTree(value, path, 0, iter.current());
          return true;
        }
      }
    };
  }

  protected JsonValue copyTree(JsonValue root, JsonValue[] path, int i, JsonValue newValue)
    throws Exception
  {
    i++;
    if( i == path.length )
    {
      return newValue;
    }

    JsonValue p = path[i];
    if( root instanceof JsonRecord )
    {
      JsonRecord in = (JsonRecord)root;
      BufferedJsonRecord out = new BufferedJsonRecord( in.size() ); // TODO: memory
      // TODO: out = in.getShallowCopy(out);
      for( Map.Entry<JsonString,JsonValue> e: in )
      {
        JsonString name = e.getKey();
        JsonValue value = e.getValue();
        if( value == p )
        {
          value = copyTree(value, path, i, newValue);
        }
        out.add(name, value);
      }
      return out;
    }
    else if( root instanceof JsonArray )
    {
      JsonArray in = (JsonArray)root;
      BufferedJsonArray out = new BufferedJsonArray(); // TODO: memory, capacity=in.count() 
      // TODO: out = in.getShallowCopy(out);
      for( JsonValue value: in )
      {
        if( value == p )
        {
          value = copyTree(value, path, i, newValue);
        }
        out.add(value);
      }
      return out;
    }
    else
    {
      // we shouldn't get here
      throw new InvalidClassException(root.getClass().getName(), "not supported");
    }
  }
}

