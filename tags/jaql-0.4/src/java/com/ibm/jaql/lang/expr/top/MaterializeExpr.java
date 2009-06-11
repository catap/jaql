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
package com.ibm.jaql.lang.expr.top;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * 
 */
public class MaterializeExpr extends TopExpr
{
  private Var var;

  /**
   * materialize()
   * 
   * @param exprs
   */
  public MaterializeExpr(Expr[] exprs)
  {
    super(NO_EXPRS);
  }

  /**
   * materialize var
   * 
   * @param var
   */
  public MaterializeExpr(Var var)
  {
    super(NO_EXPRS);
    this.var = var;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isConst()
   */
  @Override
  public boolean isConst()
  {
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(Context context) throws Exception
  {
    Item result = JBool.falseItem;
    if (var.expr != null && var.value == null )
    {
      result = JBool.trueItem;
      Context gctx = JaqlUtil.getSessionContext();
      Item value = var.expr.eval(gctx);
      var.setValue(value);
    }
    return result;
  }
}
