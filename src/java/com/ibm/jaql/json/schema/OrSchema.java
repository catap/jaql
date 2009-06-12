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

import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Bool3;

/** Schema that matches if at least one of the provided schemata matches. */
public class OrSchema extends Schema
{
  protected Schema[] schemata;    // list of alternatives, never null

  // -- construction ------------------------------------------------------------------------------
  
  public OrSchema(Schema[] schemata)
  {
    if (schemata.length == 0)
    {
      throw new IllegalArgumentException("at least one schema has to be provided");
    }
    JaqlUtil.enforceNonNull(schemata);
    this.schemata = schemata;
  }
  
  public OrSchema(Schema s1, Schema s2)
  {
    this(new Schema[] { s1, s2 });
  }
  
  /**
   * 
   */
  public OrSchema()
  {
    this(new Schema[0]);
  }

  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.OR;
  }
  
  @Override
  public Bool3 isNull()
  {
    Bool3 result = schemata[0].isNull();
    switch (result)
    {
    case TRUE:
      // check whether all are true
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].isNull().always()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.TRUE;
    
    case FALSE:
      // check whether all are false
      for (int i=1; i<schemata.length; i++) 
      {
        if (!schemata[i].isNull().never()) 
        {
          return Bool3.UNKNOWN;
        }
      }
      return Bool3.FALSE;      
    
    case UNKNOWN:
      return Bool3.UNKNOWN;
    
    default:
      throw new IllegalStateException();
    }
  }
  
  @Override
  public Bool3 isArray()
  {
    Bool3 result = Bool3.FALSE;
    for (Schema s : schemata)
    {
      result = result.or(s.isArray());
    }
    return result;
  }

  @Override
  public Bool3 isConst()
  {
    Bool3 result = Bool3.TRUE;
    for (Schema s : schemata)
    {
      result = result.and(s.isConst());
    }
    return result;
  }

  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    for (Schema s : schemata)
    {
      if (s.matches(value))
      {
        return true;
      }
    }
    return false;
  }
  
  
  // -- getters -----------------------------------------------------------------------------------
  
  public Schema[] getInternal()
  {
    return schemata;
  }
}
