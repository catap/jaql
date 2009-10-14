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

import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.Bool3;

/** Schema for the null value */
public final class NullSchema extends Schema
{
  NullSchema()
  {    
  }

  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.NULL;
  }

  @Override
  public boolean hasModifiers()
  {
    return false;
  }
  
  @Override
  public boolean isConstant()
  {
    return true;
  }

  @Override
  public Bool3 isEmpty(JsonType type, JsonType ... types) 
  {
    return is(type, types);
  }

  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    return new Class[0]; 
  }
  
  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    return value == null;
  }
  
  
  // -- merge -------------------------------------------------------------------------------------

  @Override
  protected Schema merge(Schema other)
  {
    if (other instanceof NullSchema)
    {
      return this;
    }
    return null;
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  @Override
  public int compareTo(Schema other)
  {
    int c = this.getSchemaType().compareTo(other.getSchemaType());
    if (c != 0) return c;
    
    assert other instanceof NullSchema;
    return 0;
  } 
}
