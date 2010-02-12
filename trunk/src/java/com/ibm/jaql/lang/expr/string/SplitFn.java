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
package com.ibm.jaql.lang.expr.string;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * Usage: [string] strSplit(string val, string delimitter)
 *
 */
public class SplitFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("strSplit", SplitFn.class);
    }
  }
  
  BufferedJsonArray tuple = new BufferedJsonArray();
  MutableJsonString holder = new MutableJsonString();
  
  /**
   * @param args
   */
  public SplitFn(Expr[] args)
  {
    super(args);
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.arrayOrNullSchema();
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonArray eval(final Context context) throws Exception
  {
    // TODO: need a way to avoid recomputing const exprs...
    JsonString sep = JaqlUtil.enforceNonNull((JsonString)exprs[1].eval(context));
    String del = sep.toString();
    
    JsonString str = (JsonString)exprs[0].eval(context);
    if( str == null )
    {
      return null;
    }
     
    String strVal = str.toString();
    
    String[] splits = strVal.split(del);

    tuple.clear();
    for (String s : splits)
    {
      holder.setCopy(s);
      tuple.addCopy(holder);
    }

    return tuple;
  }
}