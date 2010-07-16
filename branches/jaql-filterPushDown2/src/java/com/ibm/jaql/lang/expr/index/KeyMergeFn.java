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
package com.ibm.jaql.lang.expr.index;

import static com.ibm.jaql.json.type.JsonType.NULL;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;


/**
 * [ [key,value1] ] -> keyMerge([ [key,value2] ]) ==> [ [key, value1, value2] ]
 * 
 * Both input lists are sorted by key.
 * The inner list (expr[1]) is assumed to have distinct keys.
 *    //TODO: support duplicates?  raise error?
 * For each key/value in the outer list (expr[0])
 *   return [key, value1, value2] tuples.
 *   
 * This function only requires a single key from each list to be in memory at a time.
 */
public class KeyMergeFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("keyMerge", KeyMergeFn.class);
    }
  }
  
  public KeyMergeFn(Expr... exprs)
  {
    super(exprs);
  }

  /**
   * This expression can be applied in parallel per partition of child i.
   */
  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
  }

  /** 
   * This expression evaluates both inputs only once
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    // In serial, we evaluate both inputs once, but in parallel we evaluate multiple times.
    // FIXME: only inline trivial table computations...
    return Bool3.TRUE;
    // return i == 0 ? Bool3.TRUE : Bool3.UNKNOWN;
  }


  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonValue[] keyval = new JsonValue[2];
    final BufferedJsonArray resultArray = new BufferedJsonArray(3);
    final JsonIterator iter0 = exprs[0].iter(context);
    final JsonIterator iter1 = exprs[1].iter(context);
    
    return new JsonIterator(resultArray)
    {
      boolean end1;
      JsonValue key1;
      JsonValue val1;
      
      @Override
      public boolean moveNext() throws Exception
      {
        if (!iter0.moveNext()) 
        {
          return false;
        }
        JsonArray arr = (JsonArray)iter0.current();
        arr.getAll(keyval);
        JsonValue key0 = keyval[0];
        JsonValue val0 = keyval[1];
        JsonValue val1 = null;
        if( key0 != null && !end1 )
        {
          // while !end && (key1 == null || key1 < key0)
          //    key1,val1 = iter1.next
          while( true )
          {
            if( key1 != null )
            {
              int c = key0.compareTo( key1 );
              if( c < 0 )
              {
                val1 = null;
                break;
              }
              else if( c == 0 )
              {
                val1 = this.val1;
                break;
              }
            }
            end1 = !iter1.moveNext();
            if( end1 )
            {
              break;
            }
            arr = (JsonArray)iter1.current();
            arr.getAll(keyval);
            this.key1 = keyval[0];
            this.val1 = keyval[1];
          }
        }
        resultArray.set(0, key0);
        resultArray.set(1, val0);
        resultArray.set(2, val1);
        return true; // currentValue == resultArray
      }
    };
  }
  
  @Override
  public Schema getSchema()
  {
    // determine outer schema
    Schema outer = exprs[0].getSchema();
    Schema outerElements = SchemaTransformation.arrayElements(outer);
    if (outerElements == null)
    {
      // outer is not an array; only valid value is null 
      if (outer.is(NULL).maybe())
      {
        return SchemaFactory.emptyArraySchema();
      }
      else
      {
        throw new IllegalArgumentException("array expected as outer table for keyMerge");
      }
    }
    Schema key = outerElements.element(JsonLong.ZERO);
    Schema value1 = outerElements.element(JsonLong.ONE);
    if (key==null || value1==null)
    {
      throw new IllegalArgumentException("array of size-2 arrays expected as outer table for keyLookup");
    }
    
    // determine inner schema
    Schema inner = exprs[1].getSchema();
    Schema innerElements = SchemaTransformation.arrayElements(inner);
    Schema value2;
    if (innerElements == null)
    {
      // inner is not an array; only valid value is null 
      if (inner.is(NULL).maybe())
      {
        value2 = SchemaFactory.nullSchema();
      }
      else
      {
        throw new IllegalArgumentException("array expected as inner table for keyLookup");
      }
    }
    else
    {
      value2 = innerElements.element(JsonLong.ONE);
      if (value2==null)
      {
        throw new IllegalArgumentException("array of size-2 arrays expected as inner table for keyLookup");
      }
      value2 = SchemaTransformation.addNullability(value2);
    }
    
    // return result
    Schema resultElements = new ArraySchema(new Schema[] { key, value1, value2 });
    return new ArraySchema(null, resultElements);
  }
}
