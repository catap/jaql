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

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/** Schema for a JSON string */
public final class StringSchema extends AtomSchemaWithLength<JsonString> 
{
  // -- schema parameters -------------------------------------------------------------------------
  
  private static JsonValueParameters parameters = null; 
  
  public static JsonValueParameters getParameters()
  {
    if (parameters == null)
    {
      parameters = AtomSchemaWithLength.getParameters(SchemaFactory.stringOrNullSchema());
    }
    return parameters;
  }

  
  // -- construction ------------------------------------------------------------------------------

  public StringSchema(JsonLong length, JsonString value, JsonRecord annotation)
  {
    super(length, value, annotation);
  }

  public StringSchema(JsonLong length, JsonRecord annotation)
  {
    super(length, annotation);
  }
  
  public StringSchema(JsonLong length)
  {
    super(length);
  }
  
  public StringSchema(JsonString value, JsonRecord annotation)
  {
    super(value, annotation);
  }
  
  public StringSchema(JsonString value)
  {
    super(value);
  }
  
  public StringSchema()
  {
    super();
  }
  
  StringSchema(JsonRecord args)
  {
    this(
        (JsonLong)getParameters().argumentOrDefault(PAR_LENGTH, args),
        (JsonString)getParameters().argumentOrDefault(PAR_VALUE, args),
        (JsonRecord)getParameters().argumentOrDefault(PAR_ANNOTATION, args));
  }
  
  
  // -- superclass methods ------------------------------------------------------------------------
  
  protected long lengthOf(JsonString s)
  {
    return s.bytesLength();
  }
  
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.STRING;
  }

  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    return new Class[] { JsonString.class }; 
  }
  
  @Override
  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonString))
    {
      return false;
    }
    if (this.value != null && !this.value.equals(value))
    {
      return false;
    }    
    JsonString v = (JsonString)value;
    if (this.length != null && this.length.longValue() != v.bytesLength())
    {
      return false;
    }
    return true;
  }
  
  // -- merge -------------------------------------------------------------------------------------

  @Override
  protected Schema merge(Schema other)
  {
    if (other instanceof StringSchema)
    {
      StringSchema o = (StringSchema)other;

      // same value
      if (this.value != null && JsonUtil.equals(this.value, o.value))
      { 
        return this;
      }
      
      // different value but same length
      if (JsonUtil.equals(this.length, o.length))
      {
        return new StringSchema(this.length);
      }
      
      // totally different
      return new StringSchema();
    }
    
    return null;
  }
}
