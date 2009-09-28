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
package com.ibm.jaql.lang.expr.pragma;

import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * This is a pragma function to force const evaluation.
 */
public class ConstPragma extends Pragma
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("const", ConstPragma.class);
    }
  }
  
  protected boolean evaluated = false;
  protected JsonValue value;

  /**
   * @param exprs
   */
  public ConstPragma(Expr[] exprs)
  {
    super(exprs);
  }

  public Bool3 getProperty(ExprProperty prop, boolean deep)
  {
    // deep ignored deliberately
    return getProperty(getProperties(), prop, null);
  }
    
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = ExprProperty.createUnsafeDefaults();
    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    return result;
  }

  
  public Schema getSchema()
  {
    return exprs[0].getSchema();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception
  {
    if( !evaluated )
    {
      value = exprs[0].eval(context);
      evaluated = true;
    }
    return value;
  }
}
