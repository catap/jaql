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
package com.ibm.jaql.lang.expr.string;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JNumber;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "substring", minArgs = 2, maxArgs = 3)
public class SubstringFn extends Expr
{
  /**
   * @param exprs
   */
  public SubstringFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    JString text = (JString) exprs[0].eval(context).get();
    if (text == null)
    {
      return Item.nil;
    }
    JNumber n = (JNumber) exprs[1].eval(context).get();
    if (n == null)
    {
      return Item.nil;
    }
    String s = text.toString(); // TODO: add JString.substring() methods with target buffer
    long start = n.longValueExact();

    if (exprs.length == 3)
    {
      n = (JNumber) exprs[2].eval(context).get();
      if (n == null)
      {
        return Item.nil;
      }
      long end = n.longValueExact();
      s = s.substring((int) start, (int) end); // TODO: switch to python/js semantics?
    }
    else
    {
      s = s.substring((int) start); // TODO: switch to python/js semantics?
    }

    JString js = new JString(s); // TODO: memory
    Item result = new Item(js); // TODO: memory
    return result;
  }
}
