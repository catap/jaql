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
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;
import com.ibm.jaql.util.Bool3;

/** Schema for a binary value */
public class BinarySchema extends Schema 
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
      this.minLength = minLength==null ? JsonLong.ZERO : minLength;
      this.maxLength = maxLength;
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
  public Bool3 isNull()
  {
    return Bool3.FALSE;
  }

  @Override
  public boolean isConstant()
  {
    return false;
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
    if (!(value instanceof JsonBinary))
    {
      return false;
    }
    JsonBinary b = (JsonBinary)value;
    
    // check length
    if (!(minLength==null || b.getLength()>=minLength.value)) return false;
    if (!(maxLength==null || b.getLength()<=maxLength.value)) return false;

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
}
