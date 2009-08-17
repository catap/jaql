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
import com.ibm.jaql.io.hadoop.FromDelConverter;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import java.util.Map.Entry;

/**
 * An expression that constructs an I/O descriptor for HDFS file access.
 */
@JaqlFn(fnName="del", minArgs=1, maxArgs=2)
public class DelFn extends AbstractHandleFn
{
  private final static JsonValue TYPE = new JsonString("hdfs");
  /**
   * exprs[0]: path
   * exprs[1]: options 
   * 
   * @param exprs
   */
  public DelFn(Expr[] exprs)
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
    options.add(Adapter.FORMAT_NAME, FromDelConverter.FORMAT);
    options.add(DefaultHadoopInputAdapter.CONVERTER_NAME, FromDelConverter.CONVERTER_NAME);
    if (exprs.length > 1)
    {
      JsonValue customOptions = exprs[1].eval(context);
      if (!(customOptions instanceof JsonRecord))
      {
        throw new RuntimeException("options for del() function has to be a record");
      }
      for (Entry<JsonString, JsonValue> option : (JsonRecord)customOptions)
      {
        options.add(option.getKey(), option.getValue());
      }
    }

    BufferedJsonRecord descriptor = new BufferedJsonRecord();
    descriptor.add(Adapter.TYPE_NAME, getType());
    descriptor.add(Adapter.LOCATION_NAME, location().eval(context));
    descriptor.add(Adapter.INOPTIONS_NAME, options);
    return descriptor;
  }
}
