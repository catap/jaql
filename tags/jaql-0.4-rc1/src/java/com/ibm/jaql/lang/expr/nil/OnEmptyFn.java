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
package com.ibm.jaql.lang.expr.nil;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

@JaqlFn(fnName = "onEmpty", minArgs = 2, maxArgs = 2)
public class OnEmptyFn extends IterExpr
{

  /**
   * input array, default value
   * 
   * @param exprs
   */
  public OnEmptyFn(Expr[] exprs)
  {
    super(exprs);
  }

  public OnEmptyFn(Expr input, Expr deflt)
  {
    super(input, deflt);
  }

  @Override
  public Iter iter(final Context context) throws Exception
  {
    return new Iter() {
      Iter    iter  = exprs[0].iter(context);
      boolean didIt = false;

      public Item next() throws Exception
      {
        Item item = iter.next();
        if( item != null )
        {
          didIt = true;
          return item;
        }
        if( !didIt )
        {
          didIt = true;
          iter = exprs[1].iter(context);
          item = iter.next();
          return item;
        }
        return null;
      }
    };
  }
}
