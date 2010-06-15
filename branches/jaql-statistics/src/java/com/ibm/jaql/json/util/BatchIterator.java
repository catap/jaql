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


/** 
 * Break an iterator into blocks of size batchSize.
 * 
 * BlockIterator blockIter = new BlockIterator(iter, batchSize);
 * while( blockIter.nextBatch() )
 * {
 *   for( JsonValue val: blockIter )
 *   {
 *      ...
 *   }
 * }
 */
public final class BatchIterator extends JsonIterator
{
  protected JsonIterator iter;
  protected long batchSize;
  protected long numReturned;
  protected boolean hitEnd;
  
  public BatchIterator(JsonIterator iter, long batchSize)
  {
    this.iter = iter;
    this.batchSize = batchSize;
    if( batchSize <= 0 )
    {
      throw new IllegalArgumentException("batchSize must be a positive value");
    }
  }
  
  public boolean nextBatch()
  {
    if( hitEnd )
    {
      numReturned = batchSize;
      return false;
    }
    else
    {
      numReturned = 0;
      return true;
    }
  }
  
  @Override
  public boolean moveNext() throws Exception
  {
    if( numReturned >= batchSize )
    {
      assert numReturned == batchSize;
      return false;
    }
    if( ! iter.moveNext() )
    {
      hitEnd = true;
      return false;
    }    
    currentValue = iter.current();
    numReturned++;
    return true; 
  }
}
