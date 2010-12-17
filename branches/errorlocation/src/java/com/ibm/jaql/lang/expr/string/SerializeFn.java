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
package com.ibm.jaql.lang.expr.string;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.FastPrintBuffer;

/**
 * @jaqlDescription return a sctring representation of any value
 * Usage:
 * string serialze( value );
 */
public class SerializeFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("serialize", SerializeFn.class);
    }
  }
  
  MutableJsonString text = new MutableJsonString();
  FastPrintBuffer out = new FastPrintBuffer();
  
  /**
   * string serialize( value v )
   * 
   * @param exprs
   */
  public SerializeFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  protected JsonString evalRaw(Context context) throws Exception
  {
    // TODO: memory!!
    JsonValue value = exprs[0].eval(context);
    out.reset();
    JsonUtil.print(out, value, 0);
    out.flush();
    text.setCopy(out.toString());
    return text;
  }
}
