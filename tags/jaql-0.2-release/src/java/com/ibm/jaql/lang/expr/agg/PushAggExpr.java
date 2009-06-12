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
package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;

/**
 * 
 */
public abstract class PushAggExpr extends Expr // TODO: delete me!
{
  /**
   * @param exprs
   */
  public PushAggExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr0
   */
  public PushAggExpr(Expr expr0)
  {
    super(expr0);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public final Item eval(Context context) throws Exception
  {
    PushAgg agg = init(context);
    agg.addMore();
    return agg.eval();
  }

  /**
   * @param context
   * @return
   * @throws Exception
   */
  public abstract PushAgg init(Context context) throws Exception;
}
