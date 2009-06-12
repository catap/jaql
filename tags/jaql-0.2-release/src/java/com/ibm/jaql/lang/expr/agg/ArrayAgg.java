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
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "array", minArgs = 1, maxArgs = 1)
public final class ArrayAgg extends AlgebraicAggregate
{
  private SpillJArray array = new SpillJArray();
  
  @Override
  public Item eval(Context context) throws Exception
  {
    Iter iter = exprs[0].iter(context);
    initInitial(context);
    Item arg;
    while( (arg = iter.next()) != null )
    {
      addInitial(arg);
    }
    Item result = getFinal();
    return result;
  }

  /**
   * Override to handle nulls.
   */
  @Override
  public void evalInitial(Context context) throws Exception
  {
    Item arg = exprs[0].eval(context);
    addInitial(arg);
  }

  /**
   * @param exprs
   */
  public ArrayAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public ArrayAgg(Expr expr)
  {
    super(expr);
  }

  @Override
  public void initInitial(Context context) throws Exception
  {
    array.clear();
  }

  @Override
  public void addInitial(Item item) throws Exception
  {
    array.add(item);
  }

  @Override
  public Item getPartial() throws Exception
  {
    return new Item(array);
  }

  @Override
  public void addPartial(Item item) throws Exception
  {
    JArray array2 = (JArray)item.get();
    array.addAll(array2.iter());
  }

  @Override
  public Item getFinal() throws Exception
  {
    if( array.isEmpty() )
    {
      return Item.nil;
    }
    return getPartial();
  }
}
