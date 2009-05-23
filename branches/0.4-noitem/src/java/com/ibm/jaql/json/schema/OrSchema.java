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

/** Schema that matches if at least one of the provided schematas match. The provided 
 * schemata are not allowed to make use of their {@link Schema#nextSchema} field.
 * 
 */
public class OrSchema extends Schema
{
  protected Schema[] schemata;    // never null

  public OrSchema(Schema[] schemata)
  {
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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.schema.Schema#matches(com.ibm.jaql.json.type.Item)
   */
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
  
  public Schema[] getInternal()
  {
    return schemata;
  }
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.OR;
  }
}
