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
package com.ibm.jaql.lang.rewrite;

import java.util.ArrayList;
import java.util.Arrays;

import com.ibm.jaql.lang.expr.array.MergeFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.Expr;

// TODO: rename merge to union (ie, list union, add list concat)

/**
 * Eliminate nested merges:
 *    merge(e1,...,merge(e2,...),e3,...)
 *    ==>
 *    merge(e1,...,e2,...,e3,...)
 *     
 * If a merge only has one leg, eliminate the merge: 
 *    merge(e1)
 *    ==>
 *    e1
 *    
 * When multiple legs of a union (merge) are ArrayExpr's put them into
 * a single ArrayExpr leg:   
 *    merge(e1..., [e2,...], e3, [e4,...]) 
 *    ==>
 *    merge(e1..., e3..., [e2,...,e4,...])
 * 
 */
public class SimplifyUnion extends Rewrite
{
  public SimplifyUnion(RewritePhase phase)
  {
    super(phase, MergeFn.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr) throws Exception
  {
    MergeFn merge = (MergeFn)expr;
    
    if( merge.parent() instanceof MergeFn )
    {
      merge.replaceInParent(merge.children(), 0, merge.numChildren());
      return true;
    }
    
    if( merge.numChildren() == 1 )
    {
      merge.replaceInParent(asArray(merge.child(0)));
      return true;
    }
    
    
    Expr[] legs = merge.children();
    for(int i = 0 ; i < legs.length ; i++)
    {
      if( legs[i] instanceof ArrayExpr )
      {
        // Found the first ArrayExpr. Look for another.
        for(int j = i + 1 ; j < legs.length ; j++)
        {
          if( legs[j] instanceof ArrayExpr )
          {
            // Found a second ArrayExpr.  We will rewrite.
            // Split original legs into:
            //     newLegs = list of non-ArrayExpr
            //     arrayArgs = list of all ArrayExpr inputs
            ArrayList<Expr> newLegs = new ArrayList<Expr>();
            for(int k = 0 ; k < j ; k++)
            {
              if( k != i )
              {
                newLegs.add(legs[k]);
              }
            }
            ArrayList<Expr> arrayArgs = new ArrayList<Expr>();
            arrayArgs.addAll(Arrays.asList(legs[i].children()));
            arrayArgs.addAll(Arrays.asList(legs[j].children()));
            for(int k = j + 1 ; k < legs.length ; k++)
            {
              if( legs[k] instanceof ArrayExpr )
              {
                arrayArgs.addAll(Arrays.asList(legs[k].children()));
              }
              else
              {
                newLegs.add(legs[k]);
              }
            }
            ArrayExpr newArray = new ArrayExpr(arrayArgs);
            if( newLegs.size() == 0 )
            {
              merge.replaceInParent(newArray);
            }
            else
            {
              newLegs.add(newArray);
              merge.setChildren(newLegs.toArray(new Expr[newLegs.size()]));
            }
            return true;
          }
        }
        // Only one ArrayExpr
        return false;
      }
    }
    // No ArrayExpr
    return false;
  }
}
