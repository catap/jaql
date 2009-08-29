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

import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;

/** Schema for a binary value */
public final class BinarySchema extends Schema 
{
  // -- private variables ------------------------------------------------------------------------- 
  
  private JsonLong minLength;
  private JsonLong maxLength;

  
  // -- schema parameters -------------------------------------------------------------------------
  
  private static Parameters parameters = null; 
  
  public static Parameters getParameters()
  {
    if (parameters == null)
    {
      parameters = new Parameters(
          new JsonString[] { PAR_MIN_LENGTH, PAR_MAX_LENGTH },
          new String[]     { "long(min=0)" , "long(min=0)" },
          new JsonValue[]  { JsonLong.ZERO , null });
    }
    return parameters;
  }

  
  // -- construction ------------------------------------------------------------------------------
  
  public BinarySchema(JsonRecord args)
  {
    this(
        (JsonLong)getParameters().argumentOrDefault(PAR_MIN_LENGTH, args),
        (JsonLong)getParameters().argumentOrDefault(PAR_MAX_LENGTH, args));
  }
  
  public BinarySchema(JsonLong minLength, JsonLong maxLength)
  {
    // check arguments
    if (!SchemaUtil.checkInterval(minLength, maxLength, JsonLong.ZERO, JsonLong.ZERO))
    {
      throw new IllegalArgumentException("invalid range: " + minLength + " " + maxLength);
    }

    // store length
    if (minLength != null || maxLength != null)
    {
      this.minLength = minLength==null ? JsonLong.ZERO : (JsonLong)JsonUtil.getImmutableCopyUnchecked(minLength);
      this.maxLength = (JsonLong)JsonUtil.getImmutableCopyUnchecked(maxLength);
    }
  }
  
  BinarySchema()
  {
  }
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.BINARY;
  }

  @Override
  public boolean hasModifiers()
  {
    return (minLength != null && minLength.get() != 0) || maxLength != null;
  }

  @Override
  public boolean isConstant()
  {
    return false;
  }

  @SuppressWarnings("unchecked")
  @Override 
  public Class<? extends JsonValue>[] matchedClasses()
  {
    return new Class[] { JsonBinary.class }; 
  }
  
  @Override
  public boolean matches(JsonValue value)
  {
    if (!(value instanceof JsonBinary))
    {
      return false;
    }
    JsonBinary b = (JsonBinary)value;
    
    // check length
    if (!(minLength==null || b.bytesLength()>=minLength.get())) return false;
    if (!(maxLength==null || b.bytesLength()<=maxLength.get())) return false;

    // everything ok
    return true;
  }
  

  // -- getters -----------------------------------------------------------------------------------
  
  public JsonLong getMinLength()
  {
    return minLength;
  }
  
  public JsonLong getMaxLength()
  {
    return maxLength;
  }

  
  // -- merge -------------------------------------------------------------------------------------

  @Override
  protected Schema merge(Schema other)
  {
    if (other instanceof BinarySchema)
    {
      BinarySchema o = (BinarySchema)other;
      JsonLong minLength = SchemaUtil.min(this.minLength, o.minLength);
      JsonLong maxLength = SchemaUtil.max(this.maxLength, o.maxLength);
      return new BinarySchema(minLength, maxLength);
    }
    return null;
  }
  
  
  // -- comparison --------------------------------------------------------------------------------
  
  @Override
  public int compareTo(Schema other)
  {
    int c = this.getSchemaType().compareTo(other.getSchemaType());
    if (c != 0) return c;
    
    BinarySchema o = (BinarySchema)other;
    c = SchemaUtil.compare(this.minLength, o.minLength);
    if (c != 0) return c;
    c = SchemaUtil.compare(this.maxLength, o.maxLength);
    if (c != 0) return c;
    
    return 0;
  } 
}
