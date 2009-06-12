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

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.SingleJsonValueIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.util.Bool3;

/**
 * emptyOnNull(e) == firstNonNull(e, [])
 */
@JaqlFn(fnName = "emptyOnNull", minArgs = 1, maxArgs = 1)
public class EmptyOnNullFn extends IterExpr
{
  /**
   * item emptyOnNull(item)
   * 
   * @param exprs
   */
  public EmptyOnNullFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr0
   */
  public EmptyOnNullFn(Expr expr0)
  {
    super(new Expr[]{expr0});
  }

  @Override
  public Bool3 isEmpty()
  {
    return exprs[0].isEmpty();
  }

  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonIterator iter = exprs[0].iter(context);
    if (iter.isNull())
    {
      return new SingleJsonValueIterator(null);
    }
    return iter;
  }
}
