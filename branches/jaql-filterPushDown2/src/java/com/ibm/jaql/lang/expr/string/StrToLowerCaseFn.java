/*
 * Copyright (C) IBM Corp. 2010.
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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * Convert a string to lower case.
 */
public class StrToLowerCaseFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("strToLowerCase", StrToLowerCaseFn.class);
    }
  }
  
  protected MutableJsonString text;
  
  public StrToLowerCaseFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.stringOrNullSchema();
  }

  @Override
  public JsonString eval(Context context) throws Exception
  {
    JsonString s = (JsonString)exprs[0].eval(context);
    if( s == null )
    {
      return null;
    }
    if( text == null )
    {
      text = new MutableJsonString();
    }
    // TODO: optimize: if s is all lower case already, just return it.
    // TODO: optimize: do conversions on utf 8 bytes
    text.setCopy(s.toString().toLowerCase());
    return text;
  }
}
