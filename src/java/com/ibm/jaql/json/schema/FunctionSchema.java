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

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/** Schema for a function. TODO: much more can be done here */
public class FunctionSchema extends Schema 
{
  // -- schema parameters -------------------------------------------------------------------------
  
  private static JsonValueParameters parameters = null; 
  
  public static JsonValueParameters getParameters()
  {
    if (parameters == null)
    {
      parameters = JsonValueParameters.NONE;
    }
    return parameters;
  }
  
  // -- construction ------------------------------------------------------------------------------

  public FunctionSchema()
  {
  }

  FunctionSchema(JsonRecord args)
  {
  }
 
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.FUNCTION;
  }
  
  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    return new Class[] { Function.class }; 
  }
  
  @Override
  public boolean matches(JsonValue value)
  {
    if (!(value instanceof Function))
    {
      return false;
    }
    return true;
  }
  
  public boolean hasModifiers()
  {
    return false;
  }
  
  // -- getters -----------------------------------------------------------------------------------
  
  @Override
  public boolean isConstant()
  {
    return false;
  }
  
  public Function getConstant()
  {
    return null;
  }
  
  // -- merge -------------------------------------------------------------------------------------

  @Override
  protected Schema merge(Schema other)
  {
    if (other instanceof FunctionSchema)
    {
      return this;
    }
    return null;
  }

  // -- comparison --------------------------------------------------------------------------------

  @Override
  public int compareTo(Schema other)
  {
    return this.getSchemaType().compareTo(other.getSchemaType());    
  }
}
