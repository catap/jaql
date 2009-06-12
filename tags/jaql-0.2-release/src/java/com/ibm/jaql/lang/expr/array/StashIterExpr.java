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
package com.ibm.jaql.lang.expr.array;

import java.io.PrintStream;
import java.util.HashSet;

import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.util.Bool3;

/**
 * This expression will stream out an Iterator<Item> values.
 * This is an odd expression that cannot be properly decompiled or invoked multiple times.
 * It is used to bridge from an Iterator to an Expr when it is known that
 * it will only be invoked once and not decompiled.
 * Use will caution!
 */
public class StashIterExpr extends IterExpr
{
  protected Iter iter;
  
  /**
   * 
   * @param exprs
   */
  public StashIterExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public StashIterExpr(Iter iter)
  {
    super(NO_EXPRS);
    this.iter = iter;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
   */
  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  @Override
  public Expr clone(VarMap varMap)
  {
    throw new RuntimeException("StashIterExpr is not clonable");
  }

  @Override
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    System.err.println("warning: StashedIterExpr was decompiled, but it cannot be recompiled!");
    exprText.print(" <<stashedIter>> ");
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Iter iter(final Context context) throws Exception
  {
    if( iter == null )
    {
      throw new RuntimeException("iter never set or requested multiple times");
    }
    Iter iter2 = iter;
    iter = null;
    return iter2;
  }  
}
