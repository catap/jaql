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
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/** Schema for a boolean value */
public final class BooleanSchema extends Schema 
{
  private JsonBool value;
  
  // -- schema parameters -------------------------------------------------------------------------
  
  private static JsonValueParameters parameters = null; 
  
  public static JsonValueParameters getParameters()
  {
    if (parameters == null)
    {
      parameters = new JsonValueParameters(new JsonValueParameter(PAR_VALUE, SchemaFactory.booleanOrNullSchema(), null));
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
    this.value = (JsonBool)JsonUtil.getImmutableCopyUnchecked(value);
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
  public boolean hasModifiers()
  {
    return value != null;
  }
  
  @Override
  public boolean isConstant()
  {
    return value != null;
  }

  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    return new Class[] { JsonBool.class }; 
  }

  @Override
  public boolean matches(JsonValue value)
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
  
  // -- comparison --------------------------------------------------------------------------------
  
  @Override
  public int compareTo(Schema other)
  {
    int c = this.getSchemaType().compareTo(other.getSchemaType());
    if (c != 0) return c;
    
    BooleanSchema o = (BooleanSchema)other;
    c = JsonUtil.compare(this.value, o.value);
    if (c != 0) return c;
    
    return 0;
  } 
}
