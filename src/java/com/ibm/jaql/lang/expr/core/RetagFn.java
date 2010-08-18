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

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.util.Bool3;

/**
 * This function is used internally during the rewriting of tee().  
 * It is not intended for general use.
 * 
 * e -> retag( f1, ..., fn )
 * 
 * Exactly the same as:
 *   e -> expand ( jump($[0], f1, ..., fn)( [$[1]] ) -> transform[i,$[0]] ) 
 */
public class RetagFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par2u
  {
    public Descriptor()
    {
      super("retag", RetagFn.class);
    }
  }
  
  public RetagFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.valueOf(i == 0);
  }
  
  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
  }

// TODO:
//  @Override
//  public Schema getSchema()

  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    final Function[] funcs = new Function[exprs.length-1];
    for(int i = 0 ; i < funcs.length ; i++)
    {
      funcs[i] = (Function)exprs[i+1].eval(context);
    }
    final MutableJsonLong id = new MutableJsonLong();
    final BufferedJsonArray outPair = new BufferedJsonArray(2);
    outPair.set(0, id);
    
    return new JsonIterator(outPair)
    {
      JsonIterator iter = exprs[0].iter(context);
      JsonIterator inner = JsonIterator.EMPTY;
      final BufferedJsonArray singleton = new BufferedJsonArray(1);
      
      @Override
      public boolean moveNext() throws Exception
      {
        while( true )
        {
          if( inner.moveNext() )
          {
            JsonValue val = inner.current();
            outPair.set(1, val);
            return true;
          }
          if( !iter.moveNext() )
          {
            iter = inner = JsonIterator.EMPTY;
            return false;
          }
          JsonValue val = iter.current();
          JsonArray inPair = (JsonArray)val;
          JsonNumber id = (JsonNumber)inPair.get(0);
          outPair.set(0, id);
          int i = id.intValueExact();
          singleton.set(0, inPair.get(1));
          funcs[i].setArguments(singleton);
          inner = funcs[i].iter(context);
        }
      }
    };
  }
}
