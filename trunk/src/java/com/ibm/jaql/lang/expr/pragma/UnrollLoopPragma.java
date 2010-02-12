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
package com.ibm.jaql.lang.expr.pragma;

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * A pragma to encourage loop unrolling.
 */
public class UnrollLoopPragma extends Pragma
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("unrollLoop", UnrollLoopPragma.class);
    }
  }

  /**
   * @param exprs
   */
  public UnrollLoopPragma(Expr[] exprs)
  {
    super(exprs);
  }
  
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception
  {
    return exprs[0].eval(context);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(Context context) throws Exception
  {
    return exprs[0].iter(context);
  }
}
