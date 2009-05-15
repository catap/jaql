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
package com.ibm.jaql.lang.expr.udf;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * 
 */
public class ExternFunctionExpr extends Expr
{
  /**
   * @param exprs
   */
  public ExternFunctionExpr(Expr[] exprs)
  {
    super(exprs);
  }

  public ExternFunctionExpr(String lang, Expr externName)
  {
    super(externName);
    if( ! "java".equals(lang.toLowerCase()) ) 
    {
      throw new RuntimeException("only java functions supported right now");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JsonString className = JaqlUtil.enforceNonNull((JsonString) exprs[1].eval(context));
    ClassLoaderMgr.resolveClass(className.toString());
//    Class<?> cls = ClassLoaderMgr.resolveClass(className.toString());
    // JaqlUtil.getFunctionStore().register(fnName, className);
    return null; // TODO: do something here!!
  }
}
