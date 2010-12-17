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
package com.ibm.jaql.lang.expr.string;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * Usage: long strPos(string str, string toFind, long startIndex=0)
 *
 */
public class StrPosFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par23
  {
    public Descriptor()
    {
      super("strPos", StrPosFn.class);
    }
  }
  
  /**
   * @param args
   */
  public StrPosFn(Expr... args)
  {
    super(args);
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.longSchema();
  }
  
  protected MutableJsonLong result = new MutableJsonLong();
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  protected JsonLong evalRaw(final Context context) throws Exception
  {
    JsonString str = (JsonString)exprs[0].eval(context);
    if( str == null )
    {
      return JsonLong.MINUS_ONE;  
    }
    
    JsonString toFind = (JsonString)exprs[1].eval(context);
    if( toFind == null )
    {
      return JsonLong.MINUS_ONE;  
    }
    
    JsonNumber jstartIndex = (JsonNumber)exprs[2].eval(context);
    int startIndex = jstartIndex == null ? 0 : jstartIndex.intValue();
    
    int index = str.indexOf(toFind, startIndex);
    result.set(index);
    return result;
  }
}