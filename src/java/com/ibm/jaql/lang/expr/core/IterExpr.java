/*
 * Copyright (C) IBM Corp. 2008.
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
package com.ibm.jaql.lang.expr.core;

import java.util.ArrayList;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
public abstract class IterExpr extends Expr
{
  // Runtime state:
  protected SpillJArray tempArray;
  protected Item tempItem;

  /**
   * @param inputs
   */
  public IterExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr0
   */
  public IterExpr(Expr expr0)
  {
    super(expr0);
  }

  /**
   * @param expr0
   * @param expr1
   */
  public IterExpr(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }

  /**
   * @param expr0
   * @param expr1
   * @param expr2
   */
  public IterExpr(Expr expr0, Expr expr1, Expr expr2)
  {
    super(expr0, expr1, expr2);
  }

  /**
   * 
   * @param exprs
   */
  public IterExpr(ArrayList<? extends Expr> exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#iter(com.ibm.jaql.lang.core.Context)
   */
  public abstract Iter iter(Context context) throws Exception;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isArray()
   */
  @Override
  public final Bool3 isArray()
  {
    return Bool3.TRUE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(Context context) throws Exception
  {
    Iter iter = this.iter(context);
    if (iter.isNull())
    {
      return Item.nil;
    }
    if( tempItem == null )
    {
      tempArray = new SpillJArray();
      tempItem = new Item(tempArray);
    }
    tempArray.set(iter);
    return tempItem;
  }
}
