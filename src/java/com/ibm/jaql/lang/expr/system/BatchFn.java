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
package com.ibm.jaql.lang.expr.system;

import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * batch( [T] A , long n ) returns [[T]] 
 * 
 * Takes an array A and groups it arbitrarily into blocks of size <= n.
 * Typically the last every block but the last block has size n, but
 * batch can be run in parallel and could produce more small blocks.
 * 
 * Example:
 * 
 * range(1,10) -> batch(3);
 * ==> [ [1,2,3], [4,5,6], [7,8,9], [10] ]
 * 
 */
public class BatchFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("batch", BatchFn.class);
    }
  }
  
  public BatchFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
  }

  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonNumber jnum = (JsonNumber)exprs[1].eval(context);
    final int batchSize = jnum.intValue();
    if( batchSize < 1 )
    {
      throw new IllegalArgumentException("batchSize must be positive");
    }
    final JsonIterator input = exprs[0].iter(context);
    if( input.isNull() )
    {
      return JsonIterator.EMPTY;
    }
    final SpilledJsonArray block = new SpilledJsonArray();
    
    return new JsonIterator(block)
    {
      @Override
      public boolean moveNext() throws Exception
      {
        block.clear();
        if( ! input.moveNext() )
        {
          return false;
        }
        block.addCopy(input.current());
        for(int i = 1 ; i < batchSize && input.moveNext() ; i++)
        {
          block.addCopy(input.current());
        }
        return true;
      }
    };
  }

}
