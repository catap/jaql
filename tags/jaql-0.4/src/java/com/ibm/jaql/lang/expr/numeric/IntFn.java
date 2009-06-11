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
package com.ibm.jaql.lang.expr.numeric;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JDecimal;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JNumeric;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "int", minArgs = 1, maxArgs = 1)
public class IntFn extends Expr
{
  /**
   * int(num)
   * 
   * @param exprs
   */
  public IntFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param num
   */
  public IntFn(Expr num)
  {
    super(num);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    Item item = exprs[0].eval(context);
    JValue val = item.get();
    if (val == null)
    {
      return Item.NIL;
    }
    else if (val instanceof JLong)
    {
      return item;
    }
    else if (val instanceof JNumeric)
    {
      JNumeric n = (JNumeric) val;
      val = new JDecimal(n.longValue()); // TODO: memory
    }
    else if (val instanceof JString)
    {
      val = new JLong(val.toString()); // TODO: memory
    }
    else
    {
      throw new ClassCastException("cannot convert "
          + val.getEncoding().getType().name() + " to number");
    }
    return new Item(val); // TODO: memory
  }
}
