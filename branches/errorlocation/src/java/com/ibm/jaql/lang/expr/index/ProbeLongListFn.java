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
package com.ibm.jaql.lang.expr.index;

import static com.ibm.jaql.json.type.JsonType.NULL;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.LongArrayDedup;

/**
 * [ [long? key, any value] ] -> probeLongListFn([ long? key ]) ==> [ [long key, any value, long index] ]
 * 
 * Build a compact in-memory representation of a list of longs from the build keys (expr[1]).
 *   Nulls in the probe list are ignored and removed from the list.
 * For each key/value in the probe pairs (expr[0])
 *   index is >= 0 if the value is found in the list of keys.
 *                (actually it index of the key in the sorted list of keys, but that may change in the future)
 *            < 0  if not found 
 *                (actually it is the (-(insertion point) - 1) as defined by Arrays.binarySearch(),
 *                 but that may change in the future)
 *   return [key, value, index] tuples.
 *   
 * Note that all probe items are returned.
 * This allows us to support in and not-in predicates, as well as just simple annotations.
 * Nulls are tolerated in the probe keys, but they will never find a match.
 * Null [key,value] pairs are not tolerated; a pair is always expected.
 *
 * There is currently an implementation limit of 2B values (~16GB of memory).
 */
public class ProbeLongListFn extends IterExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("probeLongList", ProbeLongListFn.class);
    }
  }

  public ProbeLongListFn(Expr[] exprs)
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
    return Bool3.TRUE;
  }


  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    final LongArrayDedup build = new LongArrayDedup();
    for (JsonValue v : exprs[1].iter(context))
    {
      if( v != null )
      {
        JsonNumber jn = (JsonNumber)v;
        long key = jn.longValueExact();
        build.add(key);
      }
    }
    build.trimToSize(); // make room for others

    final BufferedJsonArray resultArray = new BufferedJsonArray(3);
    return new JsonIterator(resultArray)
    {
      final JsonValue[] keyval = new JsonValue[2];
      final MutableJsonLong index = new MutableJsonLong();
      JsonIterator iter = exprs[0].iter(context);
      
      @Override
      protected boolean moveNextRaw() throws Exception
      {
        if (!iter.moveNext()) 
        {
          return false;
        }
        JsonArray a = JaqlUtil.enforceNonNull((JsonArray)iter.current());
        a.getAll(keyval);
        JsonNumber jn = (JsonNumber)keyval[0];
        if( jn != null )
        {
          long key = jn.longValueExact();
          index.set( build.indexOf(key) );
        }
        else
        {
          index.set( -1 );
        }
        resultArray.set(0, keyval[0]);
        resultArray.set(1, keyval[1]);
        resultArray.set(2, index);
        return true; // currentValue == resultArray
      }
    };
  }
  
  @Override
  public Schema getSchema()
  {
    // determine probe schema
    Schema probe = exprs[0].getSchema();
    Schema probeElements = SchemaTransformation.arrayElements(probe);
    if (probeElements == null)
    {
      // probe is not an array; only valid value is null 
      if (probe.is(NULL).maybe())
      {
        return SchemaFactory.emptyArraySchema();
      }
      else
      {
        throw new IllegalArgumentException("array expected as probe table for probeLongList");
      }
    }
    Schema key = probeElements.element(JsonLong.ZERO);
    Schema value = probeElements.element(JsonLong.ONE);
    if (key==null || value==null)
    {
      throw new IllegalArgumentException("array of size-2 arrays expected as probe table for probeLongList");
    } 
    // TODO: check that key is long?
    
    // determine build schema
    // Schema build = exprs[1].getSchema();
    // TODO: check that build is long?
    
    // return result
    Schema resultElements = new ArraySchema(new Schema[] { key, value, SchemaFactory.longSchema() });
    return new ArraySchema(null, resultElements);
  }
}
