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

/** Schema for the null value */
public class NullSchema extends Schema
{
  // -- singleton ---------------------------------------------------------------------------------

  private static NullSchema theInstance = null;
  
  public static NullSchema getInstance()
  {
    if (theInstance == null)
    {
      theInstance = new NullSchema();
    }
    return theInstance;
  }
  
  private NullSchema()
  {    
  }

  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.NULL;
  }

  @Override
  public Bool3 isNull()
  {
    return Bool3.TRUE;
  }

  @Override
  public Bool3 isConst()
  {
    return Bool3.TRUE;
  }

  @Override
  public Bool3 isArray()
  {
    return Bool3.FALSE;
  }

  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    return value == null;
  }
}
