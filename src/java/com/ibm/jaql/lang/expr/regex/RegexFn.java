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

import java.util.Map;

import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * Create a regular expression (regex) value.
 */
public class RegexFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par12
  {
    public Descriptor()
    {
      super("regex", RegexFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public RegexFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    return result;
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonRegex eval(final Context context) throws Exception
  {
    JsonString regex = (JsonString) exprs[0].eval(context);
    if (regex == null)
    {
      return null;
    }
    JsonString flags = (JsonString) exprs[1].eval(context);
    return new JsonRegex(regex, flags); // TODO: memory!
  }
}
