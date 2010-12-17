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

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

// TODO: make append macro?

/**
 * append($a, $b, ...) ==> unnest [ $a, $b, ... ] NOT when $a or $b are
 * non-array (and non-null), but that's probably an improvement. NOT when $a or
 * $b are null, but the change to unnest to remove nulls will fix that should
 * append(null, null) be null? it would break any unnest definition... Push
 * unnest into ListExpr?
 */
public class AppendFn extends IterExpr
{
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par1u
  {
    public Descriptor()
    {
      super("append", AppendFn.class);
    }
  }
  
  /**
   * append(array1, array2, ...)
   * 
   * @param exprs
   */
  public AppendFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr1
   * @param expr2
   */
  public AppendFn(Expr expr1, Expr expr2)
  {
    this(new Expr[]{expr1, expr2});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    return new JsonIterator() {
      int  index = 0;
      JsonIterator iter  = JsonIterator.EMPTY;

      protected boolean moveNextRaw() throws Exception
      {
        while (true)
        {
          if (iter.moveNext()) {
            currentValue = iter.current();
            return true;
          }
          if (index >= exprs.length)
          {
            return false;
          }
          iter = exprs[index].iter(context);
          index++;
        }
      }
    };
  }
}
