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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.json.util.ScalarIter;
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
  

  public final Iter iter(final Context context) throws Exception
  {
    Item item = exprs[0].eval(context);
    if( item.isNull() )
    {
      return Iter.nil;
    }
    final Item top = new Item(item.get());  // warning: top does not own (all of) item's value
    Item hole = top;
    for(int i = 1 ; i < exprs.length ; i++)
    {
      // Share as much of input item as possible
      // but copy the expanded part so we can modify it.
      hole = ((UnrollStep)exprs[i]).expand(context, hole); 
      if( hole == null ) // input item doesn't have our path
      {
        return new ScalarIter(item);
      }
    }
    // We found the path to expand.
    final Item lastHole = hole;
    
    JArray array = (JArray)lastHole.get(); // possible intentional cast error
    
    final Iter iter = (array != null) ? array.iter() : Iter.empty;
    
    return new Iter()
    {
      boolean atStart = true;
      
      @Override
      public Item next() throws Exception
      {
        while( true )
        {
          Item item = iter.next();
          if( item == null )
          {
            if( atStart )
            {
              // Null or empty array.
              // Return item with null value
              atStart = false;
              lastHole.set(null);
              return top;
            }
            return null;
          }
          atStart = false;
          lastHole.set(item.get());
          return top;
        }
      }
    };
  }
}

