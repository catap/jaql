/*
 * Copyright (C) IBM Corp. 2008.
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

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class ReplaceElementFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par33
  {
    public Descriptor()
    {
      super("replaceElement", ReplaceElementFn.class);
    }
  }
  
  /**
   * replaceElement(array, index, value)
   * 
   * @param exprs
   */
  public ReplaceElementFn(Expr[] exprs)
  {
    super(exprs);
  }
  
  public ReplaceElementFn(Expr array, Expr index, Expr value)
  {
    super(new Expr[]{array, index, value});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonIterator tmpIter = exprs[0].iter(context);
    final JsonLong replaceIndexLong = (JsonLong) exprs[1].eval(context);
    if (replaceIndexLong == null || replaceIndexLong.get() < 0)
    {
      return tmpIter;
    }
    return new JsonIterator() {
      long index        = 0;
      long replaceIndex = replaceIndexLong.get();
      JsonIterator iter         = tmpIter;

      public boolean moveNext() throws Exception
      {
        long i = index;
        boolean hasNext = iter.moveNext();
        index++;
        if (i == replaceIndex)
        {
          currentValue = exprs[2].eval(context);
          return true;
        }
        if (hasNext) {
          currentValue = iter.current();
          return hasNext;
        }
        if (i >= replaceIndex)
        {
          return false;
        }
        iter = JsonIterator.EMPTY;
        currentValue = null;
        return true;
      }
    };
  }
}
