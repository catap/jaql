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

import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;
import com.ibm.jaql.util.Bool3;

/** Schema for a boolean value */
public class BooleanSchema extends Schema 
{
  private JsonBool value;
  
  // -- schema parameters -------------------------------------------------------------------------
  
  private static Parameters parameters = null; 
  
  public static Parameters getParameters()
  {
    if (parameters == null)
    {
      parameters = new Parameters(
          new JsonString[] { PAR_VALUE           },
          new Schema[]     { new BooleanSchema() },
          new JsonValue[]  { null                });
    }
    return parameters;
  }

  
  // -- construction ------------------------------------------------------------------------------
  
  public BooleanSchema(JsonRecord args)
  {
    this(
        (JsonBool)getParameters().argumentOrDefault(PAR_VALUE, args));
  }
  
  public BooleanSchema(JsonBool value)
  {
    this.value = value;
  }
  
  BooleanSchema()
  {
  }
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.BOOLEAN;
  }

  @Override
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  @Override
  public boolean isConstant()
  {
    return value != null;
  }

  @Override
  public Bool3 isArrayOrNull()
  {
    return Bool3.FALSE;
  }
  
  @Override
  public Bool3 isEmptyArrayOrNull()
  {
    return Bool3.FALSE;
  }
  
  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    if (!(value instanceof JsonBool))
    {
      return false;
    }
    JsonBool b = (JsonBool)value;
    
    if (this.value != null)
    {
      return b.equals(this.value);
    }
    return true;
  }
  

  // -- getters -----------------------------------------------------------------------------------
  
  public JsonBool getValue()
  {
    return value;
  }
  
  @Override
  // -- merge -------------------------------------------------------------------------------------

  protected Schema merge(Schema other)
  {
    if (other instanceof BooleanSchema)
    {
      BooleanSchema o = (BooleanSchema)other;
      if (this.value==null)
      {
        return this;
      }
      else if (o.value==null)
      {
        return o;
      }
      else if (this.value.equals(o.value)) // both non null
      {
        return this;
      }
      else // one is true, one is false 
      {
        return new BooleanSchema();
      }
    }
    return null;
  }
 
}
