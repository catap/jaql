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
import java.util.HashSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JNumber;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;


public class PathArraySlice extends PathArray
{
  /**
   * @param exprs
   */
  public PathArraySlice(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param index
   */
  public PathArraySlice(Expr start, Expr end)
  {
    super(start, end, new PathReturn());
  }

  /**
   * @param index
   * @param next
   */
  public PathArraySlice(Expr start, Expr end, Expr next)
  {
    super(start, end, next);
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("[");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(":");
    exprs[1].decompile(exprText, capturedVars);
    exprText.print("]");
    exprs[2].decompile(exprText, capturedVars);
  }


  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Iter iter(final Context context) throws Exception
  {
    JArray arr = (JArray)context.pathInput.get();
    if( arr == null )
    {
      return Iter.empty;
    }
    JNumber start = (JNumber)exprs[0].eval(context).get();
    if( start == null )
    {
      return Iter.empty;
    }
    JNumber end = (JNumber)exprs[1].eval(context).get();
    if( end == null )
    {
      return Iter.empty;
    }
    final long s = start.longValueExact();
    final long e = end.longValueExact();
    if( s > e )
    {
      return Iter.empty;
    }
    Item item;
    final Iter iter = arr.iter();
    long i;
    for( i = 0 ; i < s ; i++ )
    {
      item = iter.next();
      if( item == null )
      {
        return Iter.empty;
      }
    }
    return new Iter()
    {
      long index = s;
      
      @Override
      public Item next() throws Exception
      {
        if( index <= e )
        {
          index++;
          Item item = iter.next();
          if( item != null )
          {
            return nextStep(context, item);
          }
        }
        return null;
      }
    };
  }
}
