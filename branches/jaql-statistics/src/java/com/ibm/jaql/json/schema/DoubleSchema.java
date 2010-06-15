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

import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/** Schema for a double. */
public final class DoubleSchema extends AtomSchema<JsonDouble> 
{
  // -- schema parameters -------------------------------------------------------------------------
  
  private static JsonValueParameters parameters = null; 
  
  public static JsonValueParameters getParameters()
  {
    if (parameters == null)
    {
      parameters = AtomSchema.getParameters(SchemaFactory.doubleOrNullSchema());
    }
    return parameters;
  }

  
  // -- construction ------------------------------------------------------------------------------
  
  public DoubleSchema(JsonDouble value, JsonRecord annotation)
  {
    super(value, annotation);
  }
  
  public DoubleSchema(JsonDouble value)
  {
    super(value);
  }
  
  public DoubleSchema()
  {
    super();
  }
  
  DoubleSchema(JsonRecord args)
  {
    this(
        (JsonDouble)getParameters().argumentOrDefault(PAR_VALUE, args),
        (JsonRecord)getParameters().argumentOrDefault(PAR_ANNOTATION, args));
  }
  
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.DOUBLE;
  }

  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    return new Class[] { JsonDouble.class }; 
  }
  
  @Override
  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonDouble))
    {
      return false;
    }
    if (this.value != null && !this.value.equals(value))
    {
      return false;
    }    
    return true;
  }
  

  // -- merge -------------------------------------------------------------------------------------

  @Override
  protected Schema merge(Schema other)
  {
    if (other instanceof DoubleSchema)
    {
      DoubleSchema o = (DoubleSchema)other;
      if (JsonUtil.equals(this.value, o.value))
      { 
        return new DoubleSchema(this.value);
      }
      else
      {
        return new DoubleSchema();
      }
    }
    return null;
  }
}
