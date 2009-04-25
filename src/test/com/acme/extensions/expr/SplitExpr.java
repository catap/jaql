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
import com.ibm.jaql.json.type.SpillJArray;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "split", minArgs = 2, maxArgs = 2)
public class SplitExpr extends Expr
{
  /**
   * @param exprs
   */
  public SplitExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Item eval(final Context context) throws Exception
  {
    // evaluate this expression's input.
    Item sItem = exprs[0].eval(context);
    Item dItem = exprs[1].eval(context);

    // if there is nothing to split, return nil
    if (sItem == null)
    {
      return Item.nil;
    }

    // if there is no delimter, return nil
    if (dItem == null)
    {
      return Item.nil;
    }

    // get the input string  
    String s = ((JString) sItem.get()).toString();

    // get the delimter
    String d = ((JString) dItem.get()).toString();

    // split the string
    String[] splits = s.split(d);

    // create an array to return the result
    int numSplits = splits.length;
    SpillJArray rArr = new SpillJArray();
    for (int i = 0; i < numSplits; i++)
      rArr.addCopy(new Item(new JString(splits[i])));
    // tell the array no more items will be added (this API will change...)
    rArr.freeze();

    // return the array as an Item
    return new Item(rArr);
  }
}
