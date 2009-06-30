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

import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;


@JaqlFn(fnName = "slice", minArgs = 3, maxArgs = 3)
public class SliceFn extends IterExpr
{
  /**
   * slice(array, firstIndex, lastIndex)
   * 
   * @param exprs
   */
  public SliceFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   * @param input
   * @param firstIndex
   * @param lastIndex
   */
  public SliceFn(Expr input, Expr firstIndex, Expr lastIndex)
  {
    super(input, firstIndex, lastIndex);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonNumeric jlow  = (JsonNumeric)exprs[1].eval(context);
    JsonNumeric jhigh = (JsonNumeric)exprs[2].eval(context);
    
    final long low  = (jlow == null) ? 0 : jlow.longValueExact();
    final long high = (jhigh == null) ? Long.MAX_VALUE : jhigh.longValueExact();
    final JsonIterator iter = exprs[0].iter(context);
    
    for(long i = 0 ; i < low ; i++)
    {
      if (!iter.moveNext()) 
      {
        return JsonIterator.EMPTY;
      }
    }
    
    return new JsonIterator() 
    {
      long index = low;

      public boolean moveNext() throws Exception
      {
        if( index <= high && iter.moveNext())
        {
          currentValue = iter.current();
          index++;
          return true;
        }
        return false;
      }
    };
  }
}
