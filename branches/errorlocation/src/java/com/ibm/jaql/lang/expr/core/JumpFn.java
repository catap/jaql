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
package com.ibm.jaql.lang.expr.core;

import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * jump(i, e0, ..., en) return one of e0 to en based on i.
 * i must be exactly one of 0...n
 * Like 'if', it should only evaluate one of e0 to en.
 * 
 * Exactly the same as:
 *   if( i == 0 ) e0
 *   else if( i == 1 ) e1
 *   ...
 *   else if( i == n ) en
 *   else raise error
 */
public class JumpFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par2u
  {
    public Descriptor()
    {
      super("jump", JumpFn.class);
    }
  }
  
  public JumpFn(Expr... exprs)
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
    Schema schema = exprs[1].getSchema();
    for(int i = 2 ; i < exprs.length ; i++)
    {
      schema = OrSchema.make(schema, exprs[i].getSchema());
      // TODO: merge or exact?
      // schema = SchemaTransformation.merge(schema, exprs[i].getSchema());
    }
    return schema;
  }

  @Override
  protected JsonValue evalRaw(Context context) throws Exception
  {
    JsonNumber jnum = (JsonNumber)exprs[0].eval(context);
    int i = jnum.intValueExact();
    return exprs[i + 1].eval(context);
  }
}
