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
package com.ibm.jaql.lang.expr.array;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;


/**
 * 
 */
public class ArrayToRecordFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("arrayToRecord", ArrayToRecordFn.class);
    }
  }
  
  /**
   * @param args: array names, array values
   */
  public ArrayToRecordFn(Expr[] args)
  {
    super(args);
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.recordSchema();
  }

  // Runtime state:
  protected BufferedJsonRecord rec;
  protected JsonString[] names;
  protected JsonValue[] values;

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  protected JsonRecord evalRaw(final Context context) throws Exception
  {
    JsonArray jnames  = (JsonArray)exprs[0].eval(context);
    JsonArray jvalues = (JsonArray)exprs[1].eval(context);
    long numNames = jnames.count();
    long numValues = jvalues.count();
    if( numValues > numNames )
    {
      throw new RuntimeException("more values than names...");
    }
    // numValues <= numNames 
    int len = (int)numNames;
    if( rec == null )
    {
      rec = new BufferedJsonRecord();
      names  = new JsonString[len];
      values = new JsonValue[len];
    }
    else if( names.length < len )
    {
      names  = new JsonString[len];
      values = new JsonValue[len];
    }
    for(int i = (int)numValues ; i < len ; i++)
    {
      values[i] = null;
    }
    jnames.getAll(names);
    jvalues.getAll(values);
    rec.set(names, values, len);
    return rec;
  }
}
