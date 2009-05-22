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
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

// TODO: RegexExec: support regex () submatch  captures a la http://developer.mozilla.org/en/docs/Core_JavaScript_1.5_Guide:Working_with_Regular_Expressions:Using_Parenthesized_Substring_Matches

/**
 * 
 */
@JaqlFn(fnName = "regex_match", minArgs = 2, maxArgs = 2)
public class RegexMatchFn extends IterExpr
{
  /**
   * @param args
   */
  public RegexMatchFn(Expr[] args)
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
    
    final JsonString substr = new JsonString();
    return new JsonIterator(substr) {
      boolean done   = false;

      public boolean moveNext() throws Exception
      {
        if (done)
        {
          regex.returnMatcher(matcher);
          return false;
        }

        substr.set(matcher.group()); // TODO: memory for the String
        done = !regex.isGlobal() || !matcher.find();
        return true; // currentValue == substr
      }
    };
  }
}
