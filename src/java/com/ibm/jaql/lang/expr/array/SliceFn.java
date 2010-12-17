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

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;


public class SliceFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par33
  {
    public Descriptor()
    {
      super("slice", SliceFn.class);
    }
  }
  
  /**
   * slice(array, firstIndex, lastIndex)
   * 
   * @param exprs
   */
  public SliceFn(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * 
   * @param input
   * @param firstIndex
   * @param lastIndex
   */
  public SliceFn(Expr input, Expr firstIndex, Expr lastIndex)
  {
    super(input, firstIndex, lastIndex);
  }

  public Schema getSchema()
  {
    // only array or null inputs are accepted; ignore other types
    Schema inputSchema = exprs[0].getSchema();
    Schema outputSchema = SchemaTransformation.restrictToArrayOrNull(inputSchema);
    if (outputSchema == null)
    {
      // compile time error, be graceful for now
      return SchemaFactory.emptyArraySchema();
    }
    
    // we could try to statically evaluate the slice parameters to improve the result schema
    // but for now keep it simple
    outputSchema = SchemaTransformation.removeNullability(outputSchema); // nulls become empty arrays
    if (outputSchema == null)
    {
      return SchemaFactory.emptyArraySchema();
    }
    else
    {
      return new ArraySchema(null, outputSchema.elements());
    }
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    JsonNumber jlow  = (JsonNumber)exprs[1].eval(context);
    JsonNumber jhigh = (JsonNumber)exprs[2].eval(context);
    
    final long low  = (jlow == null) ? 0 : jlow.longValueExact();
    final long high = (jhigh == null) ? Long.MAX_VALUE : jhigh.longValueExact();
    final JsonIterator iter = exprs[0].iter(context);
    
    for(long i = 0 ; i < low ; i++)
    {
      if (!iter.moveNext()) 
      {
        return JsonIterator.EMPTY;
      }
    }
    
    return new JsonIterator() 
    {
      long index = low;

      protected boolean moveNextRaw() throws Exception
      {
        if( index <= high && iter.moveNext())
        {
          currentValue = iter.current();
          index++;
          return true;
        }
        return false;
      }
    };
  }
}
