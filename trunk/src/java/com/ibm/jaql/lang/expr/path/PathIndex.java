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
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;


public class PathIndex extends PathStep
{
  /**
   * @param exprs
   */
  public PathIndex(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param index
   */
  public PathIndex(Expr index)
  {
    super(index, new PathReturn());
  }

  /**
   * @param index
   * @param next
   */
  public PathIndex(Expr index, Expr next)
  {
    super(index, next);
  }

  /**
   * 
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
  throws Exception
  {
    exprText.print("[");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print("]");
    exprs[1].decompile(exprText, capturedVars);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Item eval(Context context) throws Exception
  {
    JArray arr = (JArray)input.get();
    if( arr == null )
    {
      return Item.nil;
    }
    JNumber index = (JNumber)exprs[0].eval(context).get();
    if( index == null )
    {
      return Item.nil;
    }
    Item value = arr.nth(index.longValueExact());
    return nextStep(context, value);
  }
}
