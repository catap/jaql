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
package com.acme.extensions.expr;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "splitArr", minArgs = 2, maxArgs = 2)
public class SplitIterExpr extends IterExpr
{
  /**
   * @param exprs
   */
  public SplitIterExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Iter iter(Context context) throws Exception
  {
    // evaluate this expression's input.
    Item sItem = exprs[0].eval(context);
    Item dItem = exprs[1].eval(context);

    // if there is nothing to split or the split is not text, return nil
    if (sItem == null || !(sItem.get() instanceof JString)) return Iter.empty;

    // if there is no delimter or the delimiter is not text, return nil
    if (dItem == null || !(dItem.get() instanceof JString)) return Iter.empty;

    // get the input string  
    String s = ((JString) sItem.get()).toString();

    // get the delimter
    String d = ((JString) dItem.get()).toString();

    // split the string
    final String[] splits = s.split(d);

    return new Iter() {
      int  i    = 0;
      int  n    = splits.length;
      Item item = new Item();

      @Override
      public Item next() throws Exception
      {
        if (i < n)
        {
          item.set(new JString(splits[i]));
          i++;
          return item;
        }
        return null;
      }
    };
  }
}
