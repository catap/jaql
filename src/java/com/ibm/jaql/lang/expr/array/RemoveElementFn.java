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
 * @jaqlDescription Remove element from array in the given position.
 * 
 * Usage:
 * array removeElement( array arr , int position);
 * 
 * 
 * @jaqlExample removeElement([1,2,3],0);
 * [ 2,3 ]
 * 
 */
public class RemoveElementFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("removeElement", RemoveElementFn.class);
    }
  }
  
  /**
   * replaceElement(array, index, value)
   * 
   * @param exprs
   */
  public RemoveElementFn(Expr[] exprs)
  {
    super(exprs);
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
    final JsonLong indexLong = (JsonLong) exprs[1].eval(context);
    if (indexLong == null || indexLong.get() < 0)
    {
      return tmpIter;
    }
    return new JsonIterator() {
      long index    = 0;
      long toDelete = indexLong.get();

      public boolean moveNext() throws Exception
      {
        boolean hasNext = tmpIter.moveNext();
        if (hasNext && index==toDelete) {
          hasNext = tmpIter.moveNext();
          index++;
        }
        index++;
        currentValue = tmpIter.current();
        return hasNext;
      }
    };
  }
}
