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
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;


public class ScriptBlock extends Expr
{

  public ScriptBlock(Expr[] exprs)
  {
    super(exprs);
  }

  public ScriptBlock(String lang, String block)
  {
    super(new ConstExpr(new JsonString(lang)), new ConstExpr(new JsonString(block)));
  }

  @Override
protected JsonValue evalRaw(Context context) throws Exception
  {
    throw new UnsupportedOperationException("scripting is disabled");
  }
}
