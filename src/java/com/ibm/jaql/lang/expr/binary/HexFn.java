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
package com.ibm.jaql.lang.expr.binary;

import java.util.Map;

import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 *  @jaqlDescription Convert a hexadecimal string into a binary string.
 *  
 *  Usage:
 *  binary hex(string str)
 */
public class HexFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("hex", HexFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public HexFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr0
   */
  public HexFn(Expr expr0)
  {
    super(expr0);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonBinary eval(Context context) throws Exception
  {
    JsonString hexString = (JsonString)exprs[0].eval(context);
    if( hexString == null )
    {
      return null;
    }
    return new JsonBinary(hexString.toString());
  }
  
  // needed for hex(...) constructor
  @Override
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    return result;
  } 
}
