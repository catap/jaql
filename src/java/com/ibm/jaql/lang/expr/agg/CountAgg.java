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
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "count", minArgs = 1, maxArgs = 1)
public final class CountAgg extends AlgebraicAggregate
{
  private long count;
  
  /**
   * @param exprs
   */
  public CountAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public CountAgg(Expr expr)
  {
    super(expr);
  }

  @Override
  public Item eval(Context context) throws Exception
  {
    Iter iter = exprs.length == 0 ? Iter.empty : exprs[0].iter(context);
    count = 0;

    while( iter.next() != null )
    {
      count++;
    }

    return new Item(new JLong(count));
  }

  @Override
  public void initInitial(Context context) throws Exception
  {
    count = 0;
  }

  @Override
  public void addInitial(Item item) throws Exception
  {
    count++;
  }

  @Override
  public Item getPartial() throws Exception
  {
    return new Item(new JLong(count));
  }

  @Override
  public void addPartial(Item item) throws Exception
  {
    JLong n = (JLong)item.get();
    count += n.value;
  }

  @Override
  public Item getFinal() throws Exception
  {
    return new Item(new JLong(count));
  }
}
