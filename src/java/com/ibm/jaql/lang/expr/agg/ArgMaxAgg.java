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
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "argmax", minArgs = 2, maxArgs = 2)
public final class ArgMaxAgg extends AlgebraicAggregate
{
  private Item max;
  private Item arg;
  private JFunction keyFn;
  private Item[] fnArgs = new Item[1];
  private Context context;
  
  
  /**
   * @param exprs 
   */
  public ArgMaxAgg(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public void initInitial(Context context) throws Exception
  {
    this.context = context;
    max = Item.NIL;
    arg = Item.NIL;
    keyFn = (JFunction)exprs[1].eval(context).get();
  }

  @Override
  public void addInitial(Item item) throws Exception
  {
    if( item.isNull() )
    {
      return;
    }
    fnArgs[0] = item;
    Item key = keyFn.eval(context,fnArgs);
    if( max == Item.NIL )
    {
      max = new Item();
      arg = new Item();
      max.setCopy(key);
      arg.setCopy(item);
    }
    else if( key.compareTo(max) > 0 )
    {
      max.setCopy(key);
      arg.setCopy(item);
    }
  }

  @Override
  public Item getPartial() throws Exception
  {
    return arg;
  }

  @Override
  public void addPartial(Item item) throws Exception
  {
    addInitial(item);
  }

  @Override
  public Item getFinal() throws Exception
  {
    return arg;
  }
}
