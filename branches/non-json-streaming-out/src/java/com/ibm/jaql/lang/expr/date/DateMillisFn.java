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
package com.ibm.jaql.lang.expr.date;

import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

@JaqlFn(fnName="dateMillis", minArgs=1, maxArgs=1)
public class DateMillisFn extends Expr
{
  public DateMillisFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public JsonLong eval(Context context) throws Exception
  {
    JsonDate d = (JsonDate)exprs[0].eval(context);
    if( d == null )
    {
      return null;
    }
    JsonLong m = new JsonLong(d.get()); // TODO: memory
    return m;
  }

}
