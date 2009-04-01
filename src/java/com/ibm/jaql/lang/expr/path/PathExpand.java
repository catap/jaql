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
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.json.util.ScalarIter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;

/**
 * 
 *  Given ctx[]path
 *  if ctx always produces an array, and path produces an array for all elements of ctx,
 *  this can be rewritten
 * 
 * @author kbeyer
 */
public class PathExpand extends PathArray
{
  /**
   * @param exprs
   */
  public PathExpand(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param next
   */
  public PathExpand()
  {
    super(new PathReturn());
  }

  /**
   * @param next
   */
  public PathExpand(Expr next)
  {
    super(next);
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("[]");
    exprs[0].decompile(exprText, capturedVars);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Iter iter(final Context context) throws Exception
  {
    final Iter outer;
    JValue val = input.get();
    if( val == null )
    {
      return Iter.empty;
    }
    else if( val instanceof JArray )
    {
      JArray arr = (JArray)input.get();
      outer = arr.iter();
    }
    else
    {
      outer = new ScalarIter(input);
    }
    return new Iter()
    {
      Iter inner = Iter.empty;
      
      @Override
      public Item next() throws Exception
      {
        while( true )
        {
          Item item;
          item = inner.next();
          if( item != null )
          {
            return item;
          }
          item = outer.next();
          if( item == null )
          {
            return null;
          }
          item = nextStep(context, item);
          JValue val = item.get();
          if( val == null )
          {
            inner = Iter.empty;
          }
          else if( val instanceof JArray )
          {
            inner = ((JArray)val).iter();
          }
          else
          {
            inner = new ScalarIter(item);
          }
        }
      }
    };
  }
}
