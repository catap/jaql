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
package com.ibm.jaql.lang.expr.core;
import java.util.ArrayList;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JaqlFunction;

@JaqlFn(fnName="tee", minArgs=1, maxArgs=Expr.UNLIMITED_EXPRS)
public class TeeExpr extends IterExpr
{

  public TeeExpr(Expr[] inputs)
  {
    super(inputs);
  }
  
  /**
   * 
   * @param inputs [BindingExpr, Expr+]
   */
  public TeeExpr(ArrayList<Expr> inputs)
  {
    super(inputs);
  }

  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonValue[] args = new JsonValue[1]; // TODO: memory
    args[0] = exprs[0].eval(context); // TODO: stream into each function!
    JsonArray arr = (JsonArray)args[0];
    for(int i = 1 ; i < exprs.length ; i++)
    {
      JaqlFunction f = (JaqlFunction)exprs[i].eval(context);
      f.eval(context, args);
    }
    return arr.iter();
//    BindingExpr b = (BindingExpr)exprs[0];
//    JArray arr = (JArray)b.inExpr().eval(context).get();
//    for(int i = 1 ; i < exprs.length ; i++)
//    {
//      context.setVar(b.var, arr.iter());
//      exprs[i].eval(context);  
//    }
//    return arr.iter();
  }

}
