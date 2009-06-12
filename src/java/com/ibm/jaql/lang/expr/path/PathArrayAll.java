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
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;


public class PathArrayAll extends PathArray
{
  /**
   * @param exprs
   */
  public PathArrayAll(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   */
  public PathArrayAll()
  {
    super(new PathReturn());
  }

  /**
   * @param next
   */
  public PathArrayAll(Expr next)
  {
    super(next);
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("[*]");
    exprs[0].decompile(exprText, capturedVars);
  }


  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Iter iter(final Context context) throws Exception
  {
    final JArray arr = (JArray)context.pathInput.get();
    if( arr == null )
    {
      return Iter.empty;
    }
    return new Iter()
    {
      final Iter iter = arr.iter();
      
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
