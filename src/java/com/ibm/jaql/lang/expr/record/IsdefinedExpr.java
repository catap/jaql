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
package com.ibm.jaql.lang.expr.record;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JBool;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
// @JaqlFn(fnName = "hasField", minArgs = 2, maxArgs = 2)
public final class IsdefinedExpr extends Expr
{
  /**
   * Expr record
   * Expr name
   * 
   * @param exprs
   */
  public IsdefinedExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param recExpr
   */
  public IsdefinedExpr(Expr recExpr, Expr name)
  {
    super(recExpr, name);
  }

  public IsdefinedExpr(Var recVar, String name)
  {
    super(new VarExpr(recVar), new ConstExpr(new JString(name)));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
   */
  @Override
  public Bool3 isNull()
  {
    return exprs[0].isNull().or(exprs[1].isNull());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(final Context context) throws Exception
  {
    JRecord rec = (JRecord) exprs[0].eval(context).get();
    if (rec == null)
    {
      return Item.nil;
    }
    JString name = (JString) exprs[1].eval(context).get();
    if (name == null)
    {
      return Item.nil;
    }
    return JBool.make(rec.findName(name) >= 0);
  }

}
