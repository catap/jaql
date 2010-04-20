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

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * Usage: long StrPosFn(string str, string toFind, long startIndex=0)
 *
 */
public class StrPosListFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par23
  {
    public Descriptor()
    {
      super("strPosList", StrPosListFn.class);
    }
  }
  
  /**
   * @param args
   */
  public StrPosListFn(Expr... args)
  {
    super(args);
  }

  @Override
  public Schema getSchema()
  {
    return new ArraySchema(null, SchemaFactory.longSchema());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonString str = (JsonString)exprs[0].eval(context);
    if( str == null )
    {
      return JsonIterator.EMPTY;  
    }
    
    final JsonString toFind = (JsonString)exprs[1].eval(context);
    if( toFind == null )
    {
      return JsonIterator.EMPTY;  
    }
    
    JsonNumber jstartIndex = (JsonNumber)exprs[2].eval(context);
    final int startIndex = jstartIndex == null ? 0 : jstartIndex.intValue();

    final MutableJsonLong result = new MutableJsonLong(); 
    return new JsonIterator(result)
    {
      protected int index = startIndex;
      
      @Override
      public boolean moveNext() throws Exception
      {
        index = str.indexOf(toFind, index);
        if( index < 0 )
        {
          return false; 
        }
        result.set(index);
        index++;
        return true;
      }
    };
  }
}