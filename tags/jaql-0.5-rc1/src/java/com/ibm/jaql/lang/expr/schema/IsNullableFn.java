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
package com.ibm.jaql.lang.expr.schema;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * isNullable(schema): true if schema might match null, false otherwise
 */
public class IsNullableFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("isNullable", IsNullableFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public IsNullableFn(Expr... exprs)
  {
    super(exprs);
  }


  /**
   * schemaof(e) never evaluates e
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  public JsonBool eval(final Context context) throws Exception
  {
    JsonSchema jschema = (JsonSchema)exprs[0].eval(context);
    if( jschema == null )
    {
      return JsonBool.FALSE;
    }
    Schema schema = jschema.get();
    if( schema.is(JsonType.NULL).maybe() )
    {
      return JsonBool.TRUE;
    }
    return JsonBool.FALSE;
  }
}
