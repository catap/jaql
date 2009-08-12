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
package com.ibm.jaql.lang.expr.string;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "strcat", minArgs = 0, maxArgs = Expr.UNLIMITED_EXPRS)
public class StrcatFn extends Expr
{
  protected StringBuilder builder;
  protected MutableJsonString text;
  
  /**
   * string strcat(...)
   * 
   * @param exprs
   */
  public StrcatFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonString eval(Context context) throws Exception
  {
    if( text == null )
    {
      builder = new StringBuilder();
      text = new MutableJsonString();
    }
    else
    {
      builder.setLength(0);
    }
    for(Expr e: exprs)
    {
      JsonValue v = e.eval(context);
      // TODO: should arrays and records get special handling here?
      if( v != null )
      {
        String s = v.toString(); // TODO: add toJString() ?
        builder.append(s);
      }
    }
    text.setCopy(builder.toString());
    return text;
  }
}
