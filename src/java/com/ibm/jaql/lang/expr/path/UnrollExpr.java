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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;

import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.SingleJsonValueIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;

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
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("unroll ");
    for(int i = 0 ; i < exprs.length ; i++)
    {
      exprs[i].decompile(exprText, capturedVars);
    }
  }
  

  public final JsonIterator iter(final Context context) throws Exception
  {
    JsonValue value = exprs[0].eval(context);
    if( value == null )
    {
      return JsonIterator.NULL;
    }
    final JsonHolder top = new JsonHolder(value);  // warning: top does not own (all of) item's value
    JsonHolder hole = top;
    for(int i = 1 ; i < exprs.length ; i++)
    {
      // Share as much of input item as possible
      // but copy the expanded part so we can modify it.
      hole = ((UnrollStep)exprs[i]).expand(context, hole); 
      if( hole == null ) // input item doesn't have our path
      {
        return new SingleJsonValueIterator(value);
      }
    }
    // We found the path to expand.
    final JsonHolder lastHole = hole;
    
    JsonArray array = (JsonArray)lastHole.value; // possible intentional cast error
    
    final JsonIterator iter = (array != null) ? array.iter() : JsonIterator.EMPTY;
    
    return new JsonIterator()
    {
      boolean atStart = true;
      boolean eof = false;
      
      @Override
      public boolean moveNext() throws Exception // TODO: somethign seems wrong here: lastHole is updated but not used
      {
        while( true )
        {
          if (eof) 
          {
            return false;
          }
          if (!iter.moveNext()) 
          {
            eof = true;
            if( atStart )
            {
              // Null or empty array.
              // Return item with null value
              atStart = false;
              lastHole.value = null;
              currentValue = top.value;
              return true;
            }
            return false;
          }
          atStart = false;
          lastHole.value = iter.current();
          currentValue = top.value;
          return true;
        }
      }
    };
  }
}

