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

/** Schema for any value */
public class AnySchema extends Schema
{
  
  // -- singleton ---------------------------------------------------------------------------------
  
  private static AnySchema theInstance = null;
  
  public static AnySchema getInstance()
  {
    if (theInstance == null)
    {
      theInstance = new AnySchema();
    }
    return theInstance;
  }
  
  private AnySchema()
  {    
  }

  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.ANY;
  }
  
  @Override
  public Bool3 isNull()
  {
    return Bool3.UNKNOWN;
  }

  @Override
  public Bool3 isConst()
  {
    return Bool3.UNKNOWN;
  }

  @Override
  public Bool3 isArray()
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
    return true;
  }


}
