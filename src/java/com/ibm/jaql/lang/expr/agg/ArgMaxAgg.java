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
package com.ibm.jaql.lang.expr.agg;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JaqlFunction;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "argmax", minArgs = 2, maxArgs = 2)
public final class ArgMaxAgg extends AlgebraicAggregate
{
  private JsonValue max;
  private JsonValue arg;
  private JaqlFunction keyFn;
  private JsonValue[] fnArgs = new JsonValue[1];
  private Context context;
  
  
  /**
   * @param exprs 
   */
  public ArgMaxAgg(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public void initInitial(Context context) throws Exception
  {
    this.context = context;
    max = null;
    arg = null;
    keyFn = (JaqlFunction)exprs[1].eval(context);
  }

  @Override
  public void addInitial(JsonValue value) throws Exception
  {
    if( value == null )
    {
      return;
    }
    fnArgs[0] = value;
    JsonValue key = keyFn.eval(context,fnArgs);
    if( max == null )
    {
      max = key.getCopy(null);
      arg = value.getCopy(null);
    }
    else if( key.compareTo(max) > 0 )
    {
      max.setCopy(key);
      arg.setCopy(value);
    }
  }

  @Override
  public JsonValue getPartial() throws Exception
  {
    return arg;
  }

  @Override
  public void addPartial(JsonValue value) throws Exception
  {
    addInitial(value);
  }

  @Override
  public JsonValue getFinal() throws Exception
  {
    return arg;
  }
}
