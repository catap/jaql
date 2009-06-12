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

import java.util.regex.Matcher;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRegex;
import com.ibm.jaql.json.type.JSpan;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "regex_spans", minArgs = 2, maxArgs = 2)
public class RegexSpansFn extends IterExpr
{
  /**
   * @param args
   */
  public RegexSpansFn(Expr[] args)
  {
    super(args);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public Iter iter(final Context context) throws Exception
  {
    final JRegex regex = (JRegex) exprs[0].eval(context).get();
    if (regex == null)
    {
      return Iter.nil;
    }
    JString text = (JString) exprs[1].eval(context).get();
    if (text == null)
    {
      return Iter.nil;
    }
    final Matcher matcher = regex.takeMatcher();
    matcher.reset(text.toString());
    if (!matcher.find())
    {
      regex.returnMatcher(matcher);
      return Iter.empty;
    }
    return new Iter() {
      boolean done = false;
      JSpan   span = new JSpan();   // TODO: remove docid from span?
      Item    item = new Item(span);

      public Item next() throws Exception
      {
        if (done)
        {
          regex.returnMatcher(matcher);
          return null;
        }

        span.begin = matcher.start();
        int end = matcher.end();
        span.end = end - 1;
        done = !regex.isGlobal() || !matcher.find(end);
        return item;
      }
    };
  }
}
