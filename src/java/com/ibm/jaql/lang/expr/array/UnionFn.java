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

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * Union multiple arrays into one array in arbitrary order without
 * removing duplicates (like SQL's UNION ALL) 
 */
public class UnionFn extends IterExpr // TODO: add intersect, difference
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par1u
  {
    public Descriptor()
    {
      super("union", UnionFn.class);
    }
  }
  
  public UnionFn(Expr... inputs)
  {
    super(inputs);
  }
  
  public UnionFn(ArrayList<Expr> inputs)
  {
    super(inputs);
  }

  @Override
  public Schema getSchema()
  {
    Schema elems = null;
    for( int i = 0 ; i < exprs.length ; i++ )
    {
      Schema s = SchemaTransformation.restrictToArrayOrNull(exprs[i].getSchema());
      if( s == null )
      {
        throw new IllegalArgumentException("array expected for union argument "+i);
      }
      Schema e = s.elements();
      if( elems == null )
      {
        elems = e;
      }
      else
      {
        // TODO: use exact schema?
        // elems = OrSchema.make(elems, e);
        elems = SchemaTransformation.merge(elems, e);
      }
    }
    return new ArraySchema(null, elems);
  }

  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    return new JsonIterator()
    {
      int input = 0;
      JsonIterator iter = JsonIterator.EMPTY;
      
      @Override
      protected boolean moveNextRaw() throws Exception
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
