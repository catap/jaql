/*
 * Copyright (C) IBM Corp. 2010.
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

import java.util.regex.Matcher;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * @jaqlDescription Replace a substring with the replacement only if it matches the given regular expression (regex).
 * Usage: 
 * [string] strReplace(string val, regex pattern, string replacement)
 * 
 * @jaqlExample 
 * reg=regex("[a-z]+"); // define a regular expression, match at least one character.
 * val = "<abc>,<bcd>,<cde>"; // deine a string 
 * strReplace(str,reg,"a"); // replace all the match substrings with "a"
 * 
 * "<a>,<a>,<a>"
 * 
 */
public class StrReplaceFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par33
  {
    public Descriptor()
    {
      // TODO: add argument all=true
      super("strReplace", StrReplaceFn.class);
    }
  }
  
  MutableJsonString result = new MutableJsonString();
  
  /**
   * @param args
   */
  public StrReplaceFn(Expr[] args)
  {
    super(args);
  }

  @Override
  public Schema getSchema()
  {
    if( exprs[0].getSchema().is(JsonType.NULL).never() )
    {
      return SchemaFactory.stringSchema();
    }
    return SchemaFactory.stringOrNullSchema();
  }
  

  public JsonString eval(final Context context) throws Exception
  {
    JsonString jstr = (JsonString)exprs[0].eval(context);
    if( jstr == null )
    {
      return null;
    }
    JsonRegex jregex = JaqlUtil.enforceNonNull((JsonRegex)exprs[1].eval(context));
    JsonString jreplacement = (JsonString)exprs[2].eval(context);     
    String replacement = jreplacement == null ? "" : jreplacement.toString();
    Matcher matcher = jregex.takeMatcher();
    matcher.reset(jstr.toString());
    boolean all = true;
    String res;
    if( all )
    {
      res = matcher.replaceAll(replacement);
    }
    else
    {
      res = matcher.replaceFirst(replacement);
    }
    jregex.returnMatcher(matcher);
    result.setCopy(res);
    return result;
  }
}