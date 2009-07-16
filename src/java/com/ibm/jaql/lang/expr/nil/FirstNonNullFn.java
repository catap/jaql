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
package com.ibm.jaql.lang.expr.nil;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "firstNonNull", minArgs = 0, maxArgs = Expr.UNLIMITED_EXPRS)
public class FirstNonNullFn extends Expr
{
  /**
   * item firstNonNull(...)
   * 
   * @param exprs
   */
  public FirstNonNullFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr0
   * @param expr1
   */
  public FirstNonNullFn(Expr expr0, Expr expr1)
  {
    super(new Expr[]{expr0, expr1});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    for (int i = 0; i < exprs.length; i++)
    {
      Item item = exprs[i].eval(context);
      if (!item.isNull())
      {
        return item;
      }
    }
    return Item.nil;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    for (int i = 0; i < exprs.length; i++)
    {
      Iter iter = exprs[i].iter(context);
      if (!iter.isNull())
      {
        return iter;
      }
    }
    return Iter.nil;
  }
}
