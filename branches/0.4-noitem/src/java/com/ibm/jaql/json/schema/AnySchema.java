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

/** Schema that matches everything.
 * 
 */
public class AnySchema extends Schema
{
  private static final AnySchema THE_INSTANCE = new AnySchema();
  
  public static AnySchema getInstance()
  {
    return THE_INSTANCE;
  }
  
  private AnySchema()
  {    
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

  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.ANY;
  }
}
