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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "serialize", minArgs = 1, maxArgs = 1)
public class SerializeFn extends Expr
{
  MutableJsonString text = new MutableJsonString();
  ByteArrayOutputStream baos = new ByteArrayOutputStream();
  PrintStream out = new PrintStream(baos);
  
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
  public JsonString eval(Context context) throws Exception
  {
    // TODO: memory!!
    JsonValue value = exprs[0].eval(context);
    baos.reset();
    JsonUtil.print(out, value, 0);
    out.flush();
    text.set(baos.toByteArray());
    return text;
  }
}
