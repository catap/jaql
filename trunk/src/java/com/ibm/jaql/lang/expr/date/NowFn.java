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
package com.ibm.jaql.lang.expr.date;

import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "now", minArgs = 0, maxArgs = 0)
public class NowFn extends Expr
{
  /**
   * 
   */
  public NowFn()
  {
    super(NO_EXPRS);
  }

  /**
   * @param exprs
   */
  public NowFn(Expr[] exprs)
  {
    super(NO_EXPRS);
  }

  /**
   * @return
   */
  @Override
  public boolean isConst()
  {
    return false;
  }

  /**
   * @param context
   * @return
   * @throws Exception
   */
  public JsonDate eval(final Context context) throws Exception
  {
    return new JsonDate(System.currentTimeMillis()); // TODO: memory
  }
}
