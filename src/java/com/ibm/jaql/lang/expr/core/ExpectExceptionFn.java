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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.ClassLoaderMgr;

/**
 * This function is used by tests to mask expected exceptions.
 */
public class ExpectExceptionFn extends Expr
{
  protected static final Log LOG = LogFactory.getLog(ExpectExceptionFn.class.getName());
  public static final JsonString EXPECTED = new JsonString("Expected exception occured");
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par1u
  {
    public Descriptor()
    {
      super("expectException", ExpectExceptionFn.class);
    }
  }
  
  public ExpectExceptionFn(Expr... exprs)
  {
    super(exprs);
  }
  
  @Override
  public JsonValue eval(final Context context) throws Exception
  {
    try
    {
      JsonValue value = exprs[0].eval(context);
      throw new RuntimeException("Expected exception but found "+value);
    }
    catch( Exception ex )
    {
      if( exprs.length == 1 )
      {
        LOG.info("any exception expected", ex);
        return EXPECTED;
      }
      Throwable cause = ex;
      while( cause.getCause() != null )
      {
        cause = cause.getCause();
      }
      Class<?> causeClass = cause.getClass();
      for(int i = 1 ; i < exprs.length ; i++)
      {
        JsonString js = (JsonString)exprs[1].eval(context);
        Class<?> cls = ClassLoaderMgr.resolveClass(js.toString());
        if( cls.isAssignableFrom(causeClass) )
        {
          LOG.info("exception was expected "+cls.getName().toString(), ex);
          return EXPECTED;
        }
      }
      throw new RuntimeException("expected an exception, but not this one", ex);
    }
  }
}
