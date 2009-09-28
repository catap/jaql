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

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.util.JaqlUtil;

/** Converts an input value (string, array of strings or record with string values) to 
 * the specified types. */
public class ConvertFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("convert", ConvertFn.class);
    }
  }
  
  // -- variables ---------------------------------------------------------------------------------
  
  Schema schema = null;
  StringConverter converter = null;
  JsonValue target;
  
  // -- construction ------------------------------------------------------------------------------

  /**
   * @param exprs
   */
  public ConvertFn(Expr[] exprs)
  {
    super(exprs);
  }

  
  // -- evaluation --------------------------------------------------------------------------------
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(final Context context) throws Exception
  {
    JsonSchema schema = JaqlUtil.enforceNonNull((JsonSchema)exprs[1].eval(context));
    if (schema.get() != this.schema)
    {
      this.converter = new StringConverter(schema.get());
      this.target = converter.createTarget();
      this.schema = schema.get();
    }
    JsonValue value = exprs[0].eval(context);
    return converter.convert(value, target);
  }
  
  
  // -- schema ------------------------------------------------------------------------------------
  
  @Override
  public Schema getSchema()
  {
    if (exprs[1].isCompileTimeComputable().always())
    {
      try
      {
        JsonSchema schema = JaqlUtil.enforceNonNull((JsonSchema)exprs[1].eval(Env.getCompileTimeContext()));
        this.converter = new StringConverter(schema.get());
        this.target = converter.createTarget();
        this.schema = schema.get();
        return schema.get();
      }
      catch (Exception e)
      {
        // ignore
      }
    }
    return SchemaFactory.anySchema();
  }

}
