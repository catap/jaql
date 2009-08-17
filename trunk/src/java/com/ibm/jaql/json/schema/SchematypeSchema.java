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
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;

/** Schema for a schema value */
public final class SchematypeSchema extends Schema 
{
  // -- private variables ------------------------------------------------------------------------- 
  
  private JsonSchema value; 
  
  // -- schema parameters -------------------------------------------------------------------------
  
  private static Parameters parameters = null; 
  
  public static Parameters getParameters()
  {
    if (parameters == null)
    {
      parameters = new Parameters(
          new JsonString[] { PAR_VALUE },
          new Schema[]     { SchemaFactory.schematypeOrNullSchema() },
          new JsonValue[]  { null });
    }
    return parameters;
  }

  
  // -- construction ------------------------------------------------------------------------------
  
  public SchematypeSchema(JsonRecord args)
  {
    this((JsonSchema)getParameters().argumentOrDefault(PAR_VALUE, args));
  }
  
  public SchematypeSchema(JsonSchema value)
  {
    assert value == null || value.get() != null;
    this.value = (JsonSchema)JsonUtil.getImmutableCopyUnchecked(value);
  }
  
  SchematypeSchema()
  {
  }
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.SCHEMATYPE;
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
    return new Class[] { JsonSchema.class }; 
  }
  
  @Override
  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonSchema))
    {
      return false;
    }
    JsonSchema s = (JsonSchema)value;
    
    // check for equality
    if (this.value != null)
    {
      return JsonUtil.equals(this.value, s);
    }

    // everything ok
    return true;
  }
  

  // -- getters -----------------------------------------------------------------------------------
  
  public JsonSchema getValue()
  {
    return value;
  }

  
  // -- merge -------------------------------------------------------------------------------------

  @Override
  protected Schema merge(Schema other)
  {
    if (other instanceof SchematypeSchema)
    {
//      AschemaSchema o = (AschemaSchema)other;
      return SchemaFactory.schematypeSchema(); // ignore value
    }
    return null;
  }
  
  
  // -- comparison --------------------------------------------------------------------------------
  
  @Override
  public int compareTo(Schema other)
  {
    int c = this.getSchemaType().compareTo(other.getSchemaType());
    if (c != 0) return c;
    
    SchematypeSchema o = (SchematypeSchema)other;    
    return SchemaUtil.compare(this.value, o.value);
  } 
}
