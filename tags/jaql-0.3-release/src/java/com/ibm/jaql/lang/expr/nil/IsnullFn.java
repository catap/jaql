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
package com.ibm.jaql.lang.expr.nil;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "isnull", minArgs = 1, maxArgs = 1)
public class IsnullFn extends Expr
{
  /**
   * @param exprs
   */
  public IsnullFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public IsnullFn(Expr expr)
  {
    this(new Expr[]{expr});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    Expr expr = exprs[0];
    boolean b;
    if (expr.isArray().always())
    {
      Iter iter = expr.iter(context);
      b = iter.isNull();
    }
    else
    {
      Item item = expr.eval(context);
      b = item.isNull();
    }
    return JBool.make(b);
  }
}
