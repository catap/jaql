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

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class RegexTestFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("regex_test", RegexTestFn.class);
    }
  }
  
  /**
   * @param args
   */
  public RegexTestFn(Expr[] args)
  {
    super(args);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonBool eval(final Context context) throws Exception
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
    return JsonBool.make(matcher.find());
  }
}
