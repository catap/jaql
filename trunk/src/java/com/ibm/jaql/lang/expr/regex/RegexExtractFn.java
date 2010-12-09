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

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

// TODO: RegexExec: support regex () submatch  captures a la http://developer.mozilla.org/en/docs/Core_JavaScript_1.5_Guide:Working_with_Regular_Expressions:Using_Parenthesized_Substring_Matches

/**
 * @jaqlDescription Capture every first substrings which match each group (A group is a pair of parentheses used to 
 * group subpatterns.) specified in the regular expression. Return a string array like :
 *  ["match_group1", "match_group2" , "match_group3" ...]
 * 
 * Usage:
 * [string] regex_extract(regex reg, string text)
 * 
 * reg is the regular expression, text is the target string. For example, given a regular expression
 *   (a(b*))+(c*)
 * it contains 3 groups:
 *   group 1: (a(b*)) 
 *   group 2: (b*) 
 *   group 3: (c*)
 * if input is "abbabcd", by use of regex_extract function, substrings matches each group(1-3) will be captured, this function
 * will return a string array, like
 *   [ "ab", "b", "c"]
 * where "ab" is the first hit matches group 1, as well as "b" to group 2, "c" to group 3.
 * 
 * @jaqlExample regex_extract(regex("(a(b*))+(c*)"),"abbabcd");
 * [ "ab", "b", "c"]
 * 
 * @jaqlExample regex_extract(regex("(a(b*))"),"abbabcd");
 * [ "abb", "bb"]
 * 
 * 
 */
public class RegexExtractFn extends Expr
{
  public final static Schema SCHEMA =   // [string?*]?
    SchemaFactory.nullable( new ArraySchema(null, SchemaFactory.nullable(SchemaFactory.stringSchema())));

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("regex_extract", RegexExtractFn.class);
    }
  }
  
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
    return SCHEMA;
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
      arr.set(i, s == null ? null : new JsonString(s)); // TODO: memory
    }
    regex.returnMatcher(matcher);
    return arr;
  }
}
