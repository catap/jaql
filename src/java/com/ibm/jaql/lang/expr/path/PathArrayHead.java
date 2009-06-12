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


public class PathArrayHead extends PathArray
{
  /**
   * @param exprs
   */
  public PathArrayHead(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param index
   */
  public PathArrayHead(Expr end)
  {
    super(end, new PathReturn());
  }

  /**
   * @param index
   * @param next
   */
  public PathArrayHead(Expr end, Expr next)
  {
    super(end, next);
  }

  /**
   * 
   * @return
   */
  public Expr lastIndex()
  {
    return exprs[0];
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("[*:");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print("]");
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
    JsonNumber end = (JsonNumber)exprs[0].eval(context);
    if( end == null )
    {
      return JsonIterator.EMPTY;
    }
    final long e = end.longValueExact();
    if( e < 0 )
    {
      return JsonIterator.EMPTY;
    }
    final JsonIterator iter = arr.iter();
    return new JsonIterator()
    {
      long index = 0;
      
      @Override
      public boolean moveNext() throws Exception
      {
        if( index <= e )
        {
          index++;
          if (iter.moveNext()) 
          {
            currentValue = nextStep(context, iter.current());
            return true;
          }
        }
        return false;
      }
    };
  }

}
