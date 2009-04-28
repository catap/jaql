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
package com.ibm.jaql.lang.expr.span;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.json.type.JSpan;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "span_overlaps", minArgs = 2, maxArgs = 2)
public class SpanOverlapsFn extends Expr
{
  /**
   * @param exprs
   */
  public SpanOverlapsFn(Expr[] exprs)
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
    JSpan x = (JSpan) exprs[0].eval(context).get();
    if (x == null)
    {
      return Item.NIL;
    }
    JSpan y = (JSpan) exprs[1].eval(context).get();
    if (y == null)
    {
      return Item.NIL;
    }
    return JBool.make(JSpan.overlaps(x, y));
  }
}
