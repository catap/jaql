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
package com.ibm.jaql.lang.expr.core;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;

/**
 * 
 */
@JaqlFn(fnName = "typeof", minArgs = 1, maxArgs = 1)
// TODO: Should this return a JSchema?
public class TypeofExpr extends Expr
{
  /**
   * @param exprs
   */
  public TypeofExpr(Expr[] exprs)
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
    Expr expr = exprs[0];
    // FIXME: the Item created here should be cached.
    if (expr.isArray().always())
    {
      Iter iter = expr.iter(context);
      if (iter.isNull())
      {
        return new Item(Item.Type.NULL.nameValue);
      }
      else
      {
        return new Item(Item.Type.ARRAY.nameValue);
      }
    }
    else
    {
      Item item = expr.eval(context);
      return new Item(item.getEncoding().type.nameValue);
    }
  }
}
