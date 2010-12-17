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
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * elementsOf(schema): if schema is (potentially) an array schema, return the schema of its elements (if any)
 */
public class ElementsOfFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("elementsOf", ElementsOfFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public ElementsOfFn(Expr... exprs)
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

  protected JsonSchema evalRaw(final Context context) throws Exception
  {
    JsonSchema jschema = (JsonSchema)exprs[0].eval(context);
    if( jschema == null )
    {
      return null;
    }
    Schema schema = SchemaTransformation.restrictTo(jschema.get(), JsonType.ARRAY);
    if( schema == null )
    {
      return null;
    }
    schema = schema.elements();
    if( schema == null )
    {
      return null;
    }
    return new JsonSchema(schema);
  }
}
