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

import java.util.HashMap;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;
import static com.ibm.jaql.json.type.JsonType.*;
/**
 * [ [key,value1] ] -> keyLookup([ [key,value2] ]) ==> [ [key, value1, value2] ]
 * 
 * Build a hash table on the inner key/value pairs (expr[1]).
 * For each key/value in the outer pairs (expr[0])
 *   return [key, value1, value2] tuples.
 *   
 * The code assumes that the inner keys are unique (or an arbitrary value is kept)
 *    //TODO: support duplicates?  raise error?
 * 
 * If the outer key does not exist in the inner set, 
 *   null is returned for the inner value.
 *   So this is preserving the outer input (left outer join)
 *   // TODO: support full outer by finding inner values that didn't join?
 * 
 *   // TODO:support spilling large inners? 
 * 
 * @author kbeyer
 */
@JaqlFn(fnName = "keyLookup", minArgs = 2, maxArgs = 2)
public class KeyLookupFn extends IterExpr
{
  public KeyLookupFn(Expr[] exprs)
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
  public JsonIterator iter(final Context context) throws Exception
  {
    final HashMap<JsonValue,JsonValue> inner = new HashMap<JsonValue,JsonValue>();
    final JsonValue[] keyval = new JsonValue[2];
    JsonIterator iter = exprs[1].iter(context);
    for (JsonValue av : iter)
    {
      JsonArray a = JaqlUtil.enforceNonNull((JsonArray)av);
      a.getAll(keyval);
      JsonValue key = JsonUtil.getCopy(keyval[0], null);
      JsonValue val = JsonUtil.getCopy(keyval[1], null);
      inner.put(key, val);
    }
    
    final BufferedJsonArray resultArray = new BufferedJsonArray(3);
    return new JsonIterator(resultArray)
    {
      
      JsonIterator iter = exprs[0].iter(context);
      
      @Override
      public boolean moveNext() throws Exception
      {
        if (!iter.moveNext()) 
        {
          return false;
        }
        JsonArray a = JaqlUtil.enforceNonNull((JsonArray)iter.current());
        a.getAll(keyval);
        JsonValue val = inner.get(keyval[0]);
        resultArray.set(0, keyval[0]);
        resultArray.set(1, keyval[1]);
        resultArray.set(2, val);
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
        throw new IllegalArgumentException("array expected as outer table for keyLookup");
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
    Schema resultElements = new ArraySchema(key, value1, value2);
    return new ArraySchema(resultElements, null, null);
  }
}
