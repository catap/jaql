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

import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
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
  public JsonString eval(final Context context) throws Exception
  {
    JsonString text = (JsonString) exprs[0].eval(context);
    if (text == null)
    {
      return null;
    }
    JsonNumber n = (JsonNumber) exprs[1].eval(context);
    if (n == null)
    {
      return null;
    }
    String s = text.toString(); // TODO: add JString.substring() methods with target buffer
    long start = n.longValueExact();

    if (exprs.length == 3)
    {
      n = (JsonNumber) exprs[2].eval(context);
      if (n == null)
      {
        return null;
      }
      long end = n.longValueExact();
      s = s.substring((int) start, (int) end); // TODO: switch to python/js semantics?
    }
    else
    {
      s = s.substring((int) start); // TODO: switch to python/js semantics?
    }

    JsonString js = new JsonString(s); // TODO: memory
    return js;
  }
}
