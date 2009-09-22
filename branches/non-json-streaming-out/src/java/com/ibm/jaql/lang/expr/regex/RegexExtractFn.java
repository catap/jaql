/*
 * Copyright (C) IBM Corp. 2009.
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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

// TODO: RegexExec: support regex () submatch  captures a la http://developer.mozilla.org/en/docs/Core_JavaScript_1.5_Guide:Working_with_Regular_Expressions:Using_Parenthesized_Substring_Matches

/**
 * 
 */
@JaqlFn(fnName = "regexExtract", minArgs = 2, maxArgs = 2)
public class RegexExtractFn extends Expr
{
  /**
   * @param args
   */
  public RegexExtractFn(Expr[] args)
  {
    super(args);
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.arrayOrNullSchema();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonArray eval(final Context context) throws Exception
  {
    final JsonRegex regex = (JsonRegex) exprs[0].eval(context);
    if (regex == null)
    {
      return null;
    }
    JsonString text = (JsonString) exprs[1].eval(context);
    if (text == null)
    {
      return null;
    }
    final Matcher matcher = regex.takeMatcher();
    matcher.reset(text.toString());
    if (!matcher.find())
    {
      regex.returnMatcher(matcher);
      return null;
    }
    int n = matcher.groupCount();    
    BufferedJsonArray arr = new BufferedJsonArray(n); // TODO: memory
    for(int i = 0 ; i < n ; i++)
    {
      String s = matcher.group(i+1);
      arr.set(i, new JsonString(s)); // TODO: memory
    }
    regex.returnMatcher(matcher);
    return arr;
  }
}
