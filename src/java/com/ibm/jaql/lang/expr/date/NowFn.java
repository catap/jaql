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
package com.ibm.jaql.lang.expr.date;

import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Return current system date time.
 * 
 * Usage:
 * date now()
 * 
 */
public class NowFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par00
  {
    public Descriptor()
    {
      super("now", NowFn.class);
    }
  }
  
  /**
   * 
   */
  public NowFn()
  {
    super(NO_EXPRS);
  }

  /**
   * @param exprs
   */
  public NowFn(Expr[] exprs)
  {
    super(NO_EXPRS);
  }

  @Override
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.IS_NONDETERMINISTIC, true);
    return result;
  }
  
  @Override
  public Schema getSchema()
  {
    return SchemaFactory.dateSchema();
  }

  /**
   * @param context
   * @return
   * @throws Exception
   */
  protected JsonDate evalRaw(final Context context) throws Exception
  {
    return new JsonDate(System.currentTimeMillis()); // TODO: memory
  }
}
