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


public class PathToArray extends PathArray
{
  /**
   * @param exprs
   */
  public PathToArray(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   */
  public PathToArray()
  {
    super(new PathReturn());
  }

  /**
   * @param next
   */
  public PathToArray(Expr next)
  {
    super(next);
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("[?]");
    exprs[0].decompile(exprText, capturedVars);
  }
  
  @Override
  public Iter iter(final Context context) throws Exception
  {
    JValue val = input.get();
    if( val == null )
    {
      return Iter.nil; // TODO: empty or nil?
    }
    final Iter iter;
    if( val instanceof JArray )
    {
      iter = ((JArray)val).iter();
    }
    else
    {
      iter = new ScalarIter(input);
    }
    return new Iter()
    {
      @Override
      public Item next() throws Exception
      {
        Item item = iter.next();
        if( item != null )
        {
          return nextStep(context, item);
        }
        return null;
      }
    };
  }

}
