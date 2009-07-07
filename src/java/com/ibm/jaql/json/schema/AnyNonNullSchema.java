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
import com.ibm.jaql.util.Bool3;

/** Schema that accepts any value but null */
public class AnyNonNullSchema extends Schema
{
  AnyNonNullSchema()
  {    
  }

  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.ANY_NON_NULL;
  }
  
  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  @Override
  public boolean isConstant()
  {
    return false;
  }

  @Override
  public Bool3 isArrayOrNull()
  {
    return Bool3.UNKNOWN;
  }

  @Override
  public Bool3 isEmptyArrayOrNull()
  {
    return Bool3.UNKNOWN;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#matches(com.ibm.jaql.json.type.Item)
   */
  @Override
  public boolean matches(JsonValue value)
  {
    return value != null;
  }
  
  // -- merge -------------------------------------------------------------------------------------

  /** 
   * Return <code>any</code> if <code>other</code> does not match null and <code>any?</code>
   * otherwise/
   */
  @Override
  protected Schema merge(Schema other)
  {
    if (other.isNull().maybe())
    {
      return SchemaFactory.anyOrNullSchema();
    }
    else
    {
      return this;
    }
  }
  
  
  // -- introspection -----------------------------------------------------------------------------
  
  @Override
  public Schema elements()
  {
    // if the actual value has elements, they could have any schema
    return SchemaFactory.anyOrNullSchema(); 
  }

  @Override
  public Bool3 hasElement(JsonValue which)
  {
    return Bool3.UNKNOWN;
  }

  @Override
  public Schema element(JsonValue which)
  {
    // if the actual value has elements, they could have any schema
    return SchemaFactory.anyOrNullSchema(); 
  }
}
