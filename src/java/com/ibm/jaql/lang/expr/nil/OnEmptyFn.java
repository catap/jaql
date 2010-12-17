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
package com.ibm.jaql.lang.expr.nil;

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

public class OnEmptyFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("onEmpty", OnEmptyFn.class);
    }
  }
  
  /**
   * input array, default value
   * 
   * @param exprs
   */
  public OnEmptyFn(Expr[] exprs)
  {
    super(exprs);
  }

  public OnEmptyFn(Expr input, Expr deflt)
  {
    super(input, deflt);
  }

  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    return new JsonIterator() {
      JsonIterator    iter  = exprs[0].iter(context);
      boolean didIt = false;

      protected boolean moveNextRaw() throws Exception
      {
        if (iter.moveNext()) {
          didIt = true;
          currentValue = iter.current();
          return true;
        }
        if( !didIt )
        {
          didIt = true;
          iter = exprs[1].iter(context);
          if (iter.moveNext()) 
          {
            currentValue = iter.current();
            return true;
          }
        }
        return false;
      }
    };
  }
}
