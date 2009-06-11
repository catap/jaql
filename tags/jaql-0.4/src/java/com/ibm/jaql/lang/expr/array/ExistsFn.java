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
package com.ibm.jaql.lang.expr.array;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

// TODO: redefine this for JSON; add empty()
/**
 * exists(null) = null exists([]) = false exists([...]) = true, when the array
 * has at least one element (even a null) exists(...) = true, when the argument
 * is not an array or a null
 */
@JaqlFn(fnName = "exists", minArgs = 1, maxArgs = 1)
public class ExistsFn extends Expr
{
  /**
   * exists(array)
   * 
   * @param exprs
   */
  public ExistsFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * exists(array)
   * 
   * @param expr
   */
  public ExistsFn(Expr expr)
  {
    super(expr);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    Iter iter;
    Expr expr = exprs[0];
    if (expr.isArray().always())
    {
      iter = expr.iter(context);
      if (iter.isNull())
      {
        return Item.NIL;
      }
    }
    else
    {
      Item item = expr.eval(context);
      JValue w = item.get();
      if (w == null)
      {
        return Item.NIL;
      }
      else if (w instanceof JArray)
      {
        iter = ((JArray) w).iter();
      }
      else
      {
        return JBool.trueItem;
      }
    }
    Item item = iter.next();
    return JBool.make(item != null);
  }
}
