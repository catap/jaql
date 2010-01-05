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
package com.ibm.jaql.json.util;

import com.ibm.jaql.json.type.BufferedJsonArray;

/** 
 * Zip a list of iterators into a list of tuples:
 *    A = [a1,a2]
 *    B = [b1,b2,b3]
 *    C = [c1]
 * produces:
 *    [ [a1,b1,c1], [a2,b2,null], [null,b3,null] ]
 */
public final class PairwiseIterator extends JsonIterator
{
  protected JsonIterator[] iters;
  protected BufferedJsonArray tuple;
  
  public PairwiseIterator(JsonIterator... iters)
  {
    this.iters = iters;
    this.currentValue = this.tuple = new BufferedJsonArray(iters.length);
  };
  
  @Override
  public boolean moveNext() throws Exception
  {
    boolean foundOne = false;
    for (int i = 0; i < iters.length; i++)
    {
      JsonIterator iter = iters[i];
      if (iter.moveNext())
      {
        foundOne = true;
        tuple.set(i, iter.current());
      }
      else
      {
        tuple.set(i, null);
      }
    }
    if (foundOne)
    {
      return true; // currentValue == tuple
    }
    return false;
  };
}
