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
package com.ibm.jaql.lang.expr.date;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JDate;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

@JaqlFn(fnName="dateMillis", minArgs=1, maxArgs=1)
public class DateMillisFn extends Expr
{
  public DateMillisFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Item eval(Context context) throws Exception
  {
    Item item = exprs[0].eval(context);
    JDate d = (JDate)item.get();
    if( d == null )
    {
      return Item.nil;
    }
    JLong m = new JLong(d.millis); // TODO: memory
    item = new Item(m); // TODO: memory
    return item;
  }

}
