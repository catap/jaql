/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang.expr.core;

import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * This function is used internally during the rewriting of tee().  
 * It is not intended for general use.
 * 
 * e -> tagFlatten( int index, int numToExpand )
 * 
 * Exactly the same as:
 *   e -> transform each x (
 *         i = x[0],
 *         v = x[1],
 *         if( i < index ) then x
 *         else if( i > index ) then [i + numToExpand-1, v]
 *         else ( assert(0 <= v[0] < numToExpand), [ v[0] + index, v[1] ] ))
 * 
 * Example:
 *   [ [0,a], [1,[0,b]], [1,[1,c]], [2,d] ] -> tagFlatten( 1, 2 )
 *  ==
 *   [ [0,a], [1,b], [2,c], [3,d] ]
 */
public class TagFlattenFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par33
  {
    public Descriptor()
    {
      super("tagFlatten", TagFlattenFn.class);
    }
  }
  
  public TagFlattenFn(Expr... exprs)
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

// TODO:
//  @Override
//  public Schema getSchema()

  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonValue jindex = exprs[1].eval(context);
    JsonValue jsize  = exprs[2].eval(context);
    final int index  = ((JsonNumber)jindex).intValueExact();
    final int lastNested   = ((JsonNumber)jsize).intValueExact() - 1;
    final MutableJsonLong id = new MutableJsonLong();
    final BufferedJsonArray outPair = new BufferedJsonArray(2);
    outPair.set(0, id);
    
    return new JsonIterator()
    {
      JsonIterator iter = exprs[0].iter(context);
      JsonValue[] pair = new JsonValue[2];
      
      @Override
      public boolean moveNext() throws Exception
      {
        if( ! iter.moveNext() )
        {
          return false;
        }
        JsonArray jpair = (JsonArray)iter.current();
        jpair.getAll(pair);
        int i = ((JsonNumber)pair[0]).intValueExact();
        if( i < 0 ) 
        {
          throw new RuntimeException("invalid tag: "+i);
        }
        if( i < index )
        {
          // leave alone: x = [x[0], x[1]]
          currentValue = jpair;
        }
        else
        {
          currentValue = outPair;
          JsonValue value = pair[1];
          if( i > index )
          {
            // shift: [x[0]+size, x[1]]
            id.set( i + lastNested );
            outPair.set(1, value);
          }
          else // i == index
          {
            // promote: [x[1][0]+index, x[1][1]]
            ((JsonArray)value).getAll(pair);
            int j = ((JsonNumber)pair[0]).intValueExact();
            if( j < 0 || j > lastNested ) 
            {
              throw new RuntimeException("invalid subtag: 0<="+j+"<="+lastNested);
            }
            id.set(i + j);
            outPair.set(1, pair[1]);
          }
        }
        return true;
      }
    };
  }
}
