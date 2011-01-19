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

import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonSpan;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Match a subset of the input, return a [begin,end] pair that indexes into the 
 * original string, where begin indicates the start index of the previous match, as well as end 
 * indicates the offset after the last character matched.
 * 
 * Usage:
 * [string] regex_extract(regex reg, string text);
 * 
 * @jaqlExample regex_spans(regex("bcd"),"abbabcd");
 * [ span(4,6) ]
 * 
 * @jaqlExample regex_spans(regex("[a-z]+"),"abbabcd");
 * [ span(0,6) ]
 * 
 */
public class RegexSpansFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("regex_spans", RegexSpansFn.class);
    }
  }
  
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
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonRegex regex = (JsonRegex) exprs[0].eval(context);
    if (regex == null)
    {
      return JsonIterator.NULL;
    }
    JsonString text = (JsonString) exprs[1].eval(context);
    if (text == null)
    {
      return JsonIterator.NULL;
    }
    final Matcher matcher = regex.takeMatcher();
    matcher.reset(text.toString());
    if (!matcher.find())
    {
      regex.returnMatcher(matcher);
      return JsonIterator.EMPTY;
    }
    
    final JsonSpan span = new JsonSpan();   // TODO: remove docid from span?
    return new JsonIterator(span) {
      boolean done = false;

      public boolean moveNext() throws Exception
      {
        if (done)
        {
          regex.returnMatcher(matcher);
          return false;
        }

        span.begin = matcher.start();
        int end = matcher.end();
        span.end = end - 1;
        done = !regex.isGlobal() || !matcher.find(end);
        return true; // currentValue == span
      }
    };
  }
}
