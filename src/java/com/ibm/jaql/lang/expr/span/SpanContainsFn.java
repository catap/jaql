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

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonSpan;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class SpanContainsFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("span_contains", SpanContainsFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public SpanContainsFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  protected JsonBool evalRaw(final Context context) throws Exception
  {
    JsonSpan x = (JsonSpan) exprs[0].eval(context);
    if (x == null)
    {
      return null;
    }
    JsonSpan y = (JsonSpan) exprs[1].eval(context);
    if (y == null)
    {
      return null;
    }
    return JsonBool.make(JsonSpan.contains(x, y));
  }
}
