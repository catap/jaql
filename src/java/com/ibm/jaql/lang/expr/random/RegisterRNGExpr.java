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
package com.ibm.jaql.lang.expr.random;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * @jaqlDescription register a random number generator
 * Usage:
 * key registerRNG( key, long seed );
 * 
 * Register an RNG with a given name, key, and a seed.
 */
public class RegisterRNGExpr extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("registerRNG", RegisterRNGExpr.class);
    }
  }
  
  /**
   * @param exprs
   */
  public RegisterRNGExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  protected JsonValue evalRaw(Context context) throws Exception
  {
    JsonValue key = exprs[0].eval(context);
    Function seed = (Function) exprs[1].eval(context);

    JaqlUtil.getRNGStore().register(key, seed);

    return key;
  }
}
