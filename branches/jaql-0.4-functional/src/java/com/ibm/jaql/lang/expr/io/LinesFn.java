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

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter;
import com.ibm.jaql.io.hadoop.FromLinesConverter;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * An expression that constructs an I/O descriptor for HDFS file access.
 */
public class LinesFn extends AbstractHandleFn
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("lines", LinesFn.class);
    }
  }
  
  private final static JsonValue TYPE = new JsonString("hdfs");
  /**
   * exprs[0]: path
   * exprs[1]: options 
   * 
   * @param exprs
   */
  public LinesFn(Expr[] exprs)
  {
    super(exprs);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.io.AbstractHandleFn#getType()
   */
  @Override
  protected JsonValue getType()
  {
    return TYPE;
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.io.AbstractHandleFn#isMapReducible()
   */
  @Override
  public boolean isMapReducible()
  {
    return true;
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonRecord eval(Context context) throws Exception
  {
    BufferedJsonRecord options = new BufferedJsonRecord();
    options.add(Adapter.FORMAT_NAME, FromLinesConverter.FORMAT);
    options.add(DefaultHadoopInputAdapter.CONVERTER_NAME, FromLinesConverter.CONVERTER_NAME);
    BufferedJsonRecord descriptor = new BufferedJsonRecord();
    descriptor.add(Adapter.TYPE_NAME, getType());
    descriptor.add(Adapter.LOCATION_NAME, location().eval(context));
    descriptor.add(Adapter.INOPTIONS_NAME, options);
    return descriptor;
  }
}
