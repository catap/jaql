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
package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "any", minArgs = 1, maxArgs = 1)
public final class PickAnyAgg extends AlgebraicAggregate // todo: rename class or fnName
{
  private Item result = new Item();
  
  /**
   * Expr aggInput, Expr N
   * @param exprs
   */
  public PickAnyAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public PickAnyAgg(Expr expr)
  {
    super(expr);
  }

  @Override
  public void initInitial(Context context) throws Exception
  {
    result.set(null);
  }

  @Override
  public void addInitial(Item item) throws Exception
  {
    if( result.isNull() )
    {
      result.copy(item);
    }
  }

  @Override
  public Item getPartial() throws Exception
  {
    return result;
  }

  @Override
  public void addPartial(Item item) throws Exception
  {
    if( result.isNull() )
    {
      result.copy(item);
    }
  }

  @Override
  public Item getFinal() throws Exception
  {
    return result;
  }
}
