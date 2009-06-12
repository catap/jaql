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

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;


public class PathArrayTail extends PathArray
{
  /**
   * @param exprs
   */
  public PathArrayTail(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param index
   */
  public PathArrayTail(Expr start)
  {
    super(start, new PathReturn());
  }

  /**
   * @param index
   * @param next
   */
  public PathArrayTail(Expr start, Expr next)
  {
    super(start, next);
  }

  /**
   * 
   * @return
   */
  public Expr firstIndex()
  {
    return exprs[0];
  }
  
  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("[");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(":*]");
    exprs[1].decompile(exprText, capturedVars);
  }


  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonArray arr = (JsonArray)input;
    if( arr == null )
    {
      return JsonIterator.EMPTY;
    }
    JsonNumber start = (JsonNumber)exprs[0].eval(context);
    if( start == null )
    {
      return JsonIterator.EMPTY;
    }
    final long s = start.longValueExact();
    final JsonIterator iter = arr.iter();
    long i;
    for( i = 0 ; i < s ; i++ )
    {
      if (!iter.moveNext())
      {
        return JsonIterator.EMPTY;
      }
    }
    return new JsonIterator()
    {
      @Override
      public boolean moveNext() throws Exception
      {
        if (iter.moveNext())
        {
          currentValue = nextStep(context, iter.current());
          return true;
        }
        return false;
      }
    };
  }

}
