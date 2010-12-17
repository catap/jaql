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
import com.ibm.jaql.json.util.PairwiseIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Combine two arrays (A,B) into one array C, assume A = [ a1,a2,a3 ... ] , B = [ b1,b2,b3 ...] , pairwise combines every
 * elements in the same position in each array, produces C = [ [a1,b1] , [a2,b2] , [a3,c3] ... ].
 * 
 * Usage:
 * array pairwise( array A , array B );
 * 
 * @jaqlExample pairwise([1,2],[3,4]);
 * [
 *  [1,3],
 *  [2,4]
 * ]
 *   
 */
public class PairwiseFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par2u
  {
    public Descriptor()
    {
      super("pairwise", PairwiseFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public PairwiseFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    final JsonIterator[] iters = new JsonIterator[exprs.length];
    for (int i = 0; i < exprs.length; i++)
    {
      iters[i] = exprs[i].iter(context);
    }
    return new PairwiseIterator(iters);
  }
}
