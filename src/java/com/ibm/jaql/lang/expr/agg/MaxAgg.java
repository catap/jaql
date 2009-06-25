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
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "max", minArgs = 1, maxArgs = 1)
public final class MaxAgg extends AlgebraicAggregate
{
  private Item max;
  
  /**
   * @param exprs
   */
  public MaxAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public MaxAgg(Expr expr)
  {
    super(expr);
  }

  @Override
  public void initInitial(Context context) throws Exception
  {
    max = Item.nil;
  }

  @Override
  public void addInitial(Item item) throws Exception
  {
    if( item.isNull() )
    {
      return;
    }
    if( max == Item.nil )
    {
      max = new Item();
      max.copy(item);
    }
    else if( item.compareTo(max) > 0 )
    {
      max.copy(item);
    }
  }

  @Override
  public Item getPartial() throws Exception
  {
    return max;
  }

  @Override
  public void addPartial(Item item) throws Exception
  {
    addInitial(item);
  }

  @Override
  public Item getFinal() throws Exception
  {
    return max;
  }
}
