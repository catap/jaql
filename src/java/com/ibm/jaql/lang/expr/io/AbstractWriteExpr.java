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
package com.ibm.jaql.lang.expr.io;

import java.util.Map;

import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;

public abstract class AbstractWriteExpr extends Expr
{
  public AbstractWriteExpr(Expr[] exprs)
  {
    super(exprs);
  }

  public AbstractWriteExpr(Expr toWrite, Expr fd)
  {
    super(toWrite, fd);
  }

  /**
   * @return
   */
  public final Expr dataExpr()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final Expr descriptor()
  {
    return exprs[1];
  }

  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.HAS_SIDE_EFFECTS, true);
    return result;
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  @Override
protected JsonValue evalRaw(Context context) throws Exception
  {
    //  evaluate the arguments
    JsonValue args = descriptor().eval(context);
    
    // get the OutputAdapter according to the type
    OutputAdapter adapter = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(args);
  
    adapter.open();
    ClosableJsonWriter writer = adapter.getWriter();
    JsonIterator iter = dataExpr().iter(context);
    for (JsonValue value : iter) 
    {
      writer.write(value);
    }
    adapter.close();
  
    return args;
  }
}
