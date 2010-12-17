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
package com.ibm.jaql.lang.expr.io;

import java.util.Map;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * An expression that constructs an I/O descriptor for jaqls temp file access.
 */
public class JaqlTempFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("jaqltemp", JaqlTempFn.class);
    }
  }
  
  private final static JsonValue TYPE = new JsonString("jaqltemp");
  /**
   * exprs[0]: path
   * exprs[1]: schema 
   * 
   * @param exprs
   */
  public JaqlTempFn(Expr[] exprs)
  {
    super(exprs[0], exprs[1]);
  }

  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    return result;
  }

  public boolean isMapReducible()
  {
    return true;
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  protected JsonRecord evalRaw(Context context) throws Exception
  {
    BufferedJsonRecord rec = new BufferedJsonRecord();
    rec.add(Adapter.TYPE_NAME, TYPE);
    rec.add(Adapter.LOCATION_NAME, exprs[0].eval(context));
    BufferedJsonRecord options = new BufferedJsonRecord();
    options.add(new JsonString("schema"), exprs[1].eval(context));
    rec.add(Adapter.OPTIONS_NAME, options);
    return rec;
  } 
}
