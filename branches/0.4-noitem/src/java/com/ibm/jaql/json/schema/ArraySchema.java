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
package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.util.Bool3;

/** Schema for an array. Has individual schemata for the first k elements and a single schema 
 * for the remaining elements, if any. */
public class ArraySchema extends Schema
{
  // If minRest == maxRest then this is a fixed-length array (head length + rest count)
  // If head == {} and minCount == maxCount == 0, then this is an empty array

  public Schema[]          head;                 // list of schemas for the first elements (never null)
  public Schema            rest;                 // schema of the remaining elements (can be null)
  public JsonLong          minRest;              // min number of values matching rest, >=0, never iff rest==null
  public JsonLong          maxRest;              // max number of items matching rest or null

  
  // -- construction ------------------------------------------------------------------------------
  
  public ArraySchema(Schema[] head, Schema rest, JsonLong minRest, JsonLong maxRest)
  {
    // assertions to discover internal misusage
    assert head != null;
    assert rest != null || (minRest==null || minRest==JsonLong.ZERO);
    assert rest != null || (maxRest==null || maxRest==JsonLong.ZERO);
    
    // check arguments
    if (!SchemaUtil.checkInterval(minRest, maxRest, JsonLong.ZERO, JsonLong.ZERO))
    {
      throw new IllegalArgumentException("array repetition out of bounds: " + minRest + " " + maxRest);
    }
    
    // init
    this.head = head;
    this.rest = rest;
    if (rest != null)
    {
      this.minRest = minRest == null ? JsonLong.ZERO : minRest;
      this.maxRest = maxRest;
    }
  }
  
  public ArraySchema(Schema schema, JsonLong minCount, JsonLong maxCount)
  {
    this(new Schema[0], schema, minCount, maxCount);
  }
  
  public ArraySchema(Schema schema)
  {
    this(new Schema[] { schema });
  }
  
  /**
   * 
   */
  public ArraySchema(Schema[] schemata)
  {
    this(schemata, null, JsonLong.ZERO, JsonLong.ZERO);
  }

  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.ARRAY;
  }
  
  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  @Override
  public Bool3 isConst()
  {
    Bool3 result = Bool3.TRUE;
    
    // check head
    for (Schema s : head)
    {
      result = result.and(s.isConst());
    }
      
    // check rest
    if (rest==null) return result;
    if (minRest != maxRest) return result.and(Bool3.UNKNOWN); 
    return result.and(rest.isConst());
  }

  @Override
  public Bool3 isArray()
  {
    return Bool3.TRUE;
  }

  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    if (!(value instanceof JsonArray))
    {
      return false;
    }
    
    // check array head
    JsonArray arr = (JsonArray) value;
    JsonIterator iter = arr.iter();
    for (Schema s : head)
    {
      if (!iter.moveNext() || !s.matches(iter.current()))
      {
        return false;
      }
    }
    
    // check rest
    if (rest==null) return !iter.moveNext();
    assert minRest != null;
    
    // check min rest
    long i = 0;
    for (; i<minRest.value; i++)
    {
      if (!iter.moveNext() || !rest.matches(iter.current()))
      {
        return false;
      }
    }

    // check remaining rest
    assert i==minRest.value;
    for (; ; i++)
    {
      if (!iter.moveNext())
      {
        return true;
      }
      if (maxRest != null && i>=maxRest.value)
      { 
        return false;
      }
      if (!rest.matches(iter.current()))
      {
        return false;
      }
      
    }
  }

  
  // -- getters -----------------------------------------------------------------------------------
  
  public Schema[] getHeadSchemata()
  {
    return head;
  }
  
  public Schema getRestSchema()
  {
    return rest;
  }
  
  public boolean hasRest() 
  {
    return rest != null && (maxRest == null || maxRest.value > 0);    
  }
  
  public boolean isEmpty()
  {
    return head.length == 0 && !hasRest();
  }
  
  public JsonLong getMinRest() 
  {
    return minRest;    
  }
  
  public JsonLong getMaxRest()
  {
    return maxRest;
  }
}
