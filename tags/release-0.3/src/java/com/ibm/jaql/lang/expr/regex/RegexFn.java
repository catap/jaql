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
package com.ibm.jaql.lang.expr.regex;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRegex;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
@JaqlFn(fnName = "regex", minArgs = 1, maxArgs = 2)
public class RegexFn extends Expr
{
  /**
   * @param exprs
   */
  public RegexFn(Expr[] exprs)
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
    JString regex = (JString) exprs[0].eval(context).get();
    if (regex == null)
    {
      return Item.nil;
    }
    JString flags = JaqlUtil.emptyString;
    if (exprs.length == 2)
    {
      flags = (JString) exprs[1].eval(context).get();
      if (flags == null)
      {
        flags = JaqlUtil.emptyString;
      }
    }
    return new Item(new JRegex(regex, flags)); // TODO: memory!
  }
}