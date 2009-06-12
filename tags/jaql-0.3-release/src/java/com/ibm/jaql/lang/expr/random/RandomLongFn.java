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
package com.ibm.jaql.lang.expr.random;

import java.util.Random;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JNumber;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "randomLong", minArgs = 1, maxArgs = 1)
public class RandomLongFn extends Expr
{
  private Random rng;
  private JLong  longType = new JLong();
  private Item   longItem = new Item(longType);

  /**
   * long randomLong(number seed)
   * 
   * @param exprs
   */
  public RandomLongFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param seed
   */
  public RandomLongFn(Expr seed)
  {
    super(new Expr[]{seed});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isConst()
   */
  @Override
  public boolean isConst()
  {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    // FIXME: This class does not work in recursion...
    if (rng == null)
    {
      JNumber seedItem = (JNumber) (exprs[0].eval(context).get());
      long seed = seedItem.longValue();
      rng = new Random(seed);
    }
    longType.value = rng.nextLong() & 0x7FFFFFFFFFFFFFFFL;
    return longItem;
  }
}
