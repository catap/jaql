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

import java.util.ArrayList;

import com.ibm.jaql.json.type.FixedJArray;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.agg.SumAgg.Summer;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * vectorSum(array x) = [sum(x1), sum(x2), ..., sum(xn)]
 */
@JaqlFn(fnName = "vectorSum", minArgs = 1, maxArgs = 1)
public class VectorSumAgg extends AlgebraicAggregate
{
  private ArrayList<Summer> summers;
  
  /**
   * one arg
   * @param exprs
   */
  public VectorSumAgg(Expr[] exprs)
  {
    super(exprs);
  }

  public VectorSumAgg(Expr arg)
  {
    super(arg);
  }
  
  @Override
  public void initInitial(Context context) throws Exception
  {
    summers = new ArrayList<Summer>();
  }

  @Override
  public void addInitial(Item item) throws Exception
  {
    if( item.isNull() )
    {
      return;
    }
    JArray arr = (JArray)item.get();
    for( long n = arr.count() - summers.size() ; n > 0 ; n-- )
    {
      summers.add(new Summer());
    }
    Iter iter = arr.iter();
    int k = 0;
    while( (item = iter.next()) != null )
    {
      summers.get(k++).add(item);
    }
  }

  @Override
  public Item getPartial() throws Exception
  {
    int n = summers.size();
    FixedJArray arr = new FixedJArray(n);
    for(int i = 0 ; i < n ; i++)
    {
      Item item = summers.get(i).get();
      arr.set(i, item);
    }
    return new Item(arr);
  }

  @Override
  public void addPartial(Item item) throws Exception
  {
    addInitial(item);
  }

  @Override
  public Item getFinal() throws Exception
  {
    return getPartial();
  }

}
