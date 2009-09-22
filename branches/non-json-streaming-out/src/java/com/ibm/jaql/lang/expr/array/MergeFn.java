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
import java.util.ArrayList;

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * Merge multiple pipes into one pipe in arbitrary order (like SQL's UNION ALL) 
 * 
 * @author kbeyer
 */
@JaqlFn(fnName="merge", minArgs=1, maxArgs=Expr.UNLIMITED_EXPRS)
public class MergeFn extends IterExpr // TODO: add intersect, union, difference, concat
{

  public MergeFn(Expr[] inputs)
  {
    super(inputs);
  }
  
  public MergeFn(ArrayList<Expr> inputs)
  {
    super(inputs);
  }

  public MergeFn(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }


  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    return new JsonIterator()
    {
      int input = 0;
      JsonIterator iter = JsonIterator.EMPTY;
      
      @Override
      public boolean moveNext() throws Exception
      {
        while( true )
        {
          if (iter.moveNext()) {
            currentValue = iter.current();
            return true;
          }
          if( input >= exprs.length )
          {
            return false;
          }
          iter = exprs[input++].iter(context);
        }
      }
    };
  }

}
