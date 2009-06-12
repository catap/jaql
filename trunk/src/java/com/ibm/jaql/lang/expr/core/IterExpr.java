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

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
public abstract class IterExpr extends Expr
{
  protected SpilledJsonArray tempArray;

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

  /* never returns null
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#iter(com.ibm.jaql.lang.core.Context)
   */
  public abstract JsonIterator iter(Context context) throws Exception;

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
  public JsonValue eval(Context context) throws Exception
  {
    JsonIterator iter = this.iter(context);
    if (iter.isNull())
    {
      return null;
    }
    if( tempArray == null )
    {
      tempArray = new SpilledJsonArray();
    }
    tempArray.setCopy(iter);
    return tempArray;
  }
}
