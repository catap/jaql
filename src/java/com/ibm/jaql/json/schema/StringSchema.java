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

import java.util.regex.Pattern;

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;
import com.ibm.jaql.util.Bool3;

/** Schema for a JSON string */
public class StringSchema extends Schema
{
  private JsonLong minLength = JsonLong.ZERO;
  private JsonLong maxLength;
  private JsonString pattern;
  private JsonString value;
  
  // to make matching more efficient
  private Pattern compiledPattern;
  
  // -- schema parameters -------------------------------------------------------------------------

  // TODO: format parameter

  public static final JsonString PAR_PATTERN = new JsonString("pattern");
  
  private static Parameters parameters = null; 
  
  public static Parameters getParameters()
  {
    if (parameters == null)
    {
      Schema long0 = new LongSchema(JsonLong.ZERO, null, null);
      Schema string = new StringSchema();
      parameters = new Parameters(
          new JsonString[] { PAR_MIN_LENGTH, PAR_MAX_LENGTH, PAR_PATTERN, PAR_VALUE },
          new Schema[]     { long0         , long0         , string     , string },
          new JsonValue[]  { JsonLong.ZERO , null          , null       , null });
    }
    return parameters;
  }
  

  // -- construction ------------------------------------------------------------------------------
  
  public StringSchema(JsonRecord args) 
  { 
    this(
        args != null && args.containsKey(PAR_MIN_LENGTH) // to distingush whether minlENGTH IS SPECIFIED OR NOT 
          ? (JsonLong)args.get(PAR_MIN_LENGTH) 
          : null, 
        (JsonLong)getParameters().argumentOrDefault(PAR_MAX_LENGTH, args),
        (JsonString)getParameters().argumentOrDefault(PAR_PATTERN, args),
        (JsonString)getParameters().argumentOrDefault(PAR_VALUE, args));
  }
  
  public StringSchema() 
  {
  }
  
  public StringSchema(JsonLong minLength, JsonLong maxLength, JsonString pattern, JsonString value)
  {
    // check arguments
    if (!SchemaUtil.checkInterval(minLength, maxLength, JsonLong.ZERO, JsonLong.ZERO))
    {
      throw new IllegalArgumentException("invalid range: " + minLength + " " + maxLength);
    }

    // store pattern
    if (pattern != null)
    {
      this.pattern = pattern;
      compiledPattern = Pattern.compile(pattern.toString());
    }
    
    // store length
    if (minLength != null || maxLength != null)
    {
      this.minLength = minLength==null ? JsonLong.ZERO : minLength;
      this.maxLength = maxLength;
    }
    
    // store value
    if (value != null)
    {
      boolean matches;
      try
      {
        matches = matches(value);
      } catch (Exception e)
      {
        matches=false;
      }
      if (!matches)
      {
        throw new IllegalArgumentException("value argument conflicts with other arguments");
      }        
      this.value = value;
      
      // throw away other stuff
      this.minLength = JsonLong.ZERO; 
      this.maxLength = null;
      this.pattern = null;
      this.compiledPattern = null;
    }
  }
  
  public StringSchema(JsonLong minLength, JsonLong maxLength)
  {
    this(minLength, maxLength, null, null);    
  }
  
  // -- schema methods ----------------------------------------------------------------------------
  
  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.STRING;
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
    JsonString s = (JsonString)value;

    // check constant
    if (this.value != null)
    {
      return this.value.equals(value);
    }
    
    // check string length
    // TODO: currently uses UTF8 representation
    if (!(minLength==null || s.lengthUtf8()>=minLength.get())) return false;
    if (!(maxLength==null || s.lengthUtf8()<=maxLength.get())) return false;

    // check regexp pattern
    if (!(pattern == null || compiledPattern.matcher(s.toString()).matches())) return false;

    // everythink ok
    return true;
  }
  
  
  // -- getters -----------------------------------------------------------------------------------

  public JsonString getValue()
  {
    return value;
  }

  public JsonString getPattern()
  {
    return pattern;
  }
  
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
    if (other instanceof StringSchema)
    {
      StringSchema o = (StringSchema)other;
      if (this.value != null && o.value != null && this.value.equals(o.value))
      {
        return this; // both represent the same constant
      }
      
      // simple first cut; more sophisticated methods possible (not sure if needed)
      // currently information in value/pattern is ignored
      JsonLong minLength = SchemaUtil.min(this.value==null ? this.minLength : new JsonLong(this.value.lengthUtf8()), 
                                          o.value==null ? o.minLength : new JsonLong(o.value.lengthUtf8()));
      JsonLong maxLength = SchemaUtil.max(this.value==null ? this.maxLength : new JsonLong(this.value.lengthUtf8()), 
                                          o.value==null ? o.maxLength : new JsonLong(o.value.lengthUtf8()));
      return new StringSchema(minLength, maxLength, null, null);
    }
    return null;
  }
  
  // -- comparison --------------------------------------------------------------------------------
  
  @Override
  public int compareTo(Schema other)
  {
    int c = this.getSchemaType().compareTo(other.getSchemaType());
    if (c != 0) return c;
    
    StringSchema o = (StringSchema)other;
    c = SchemaUtil.compare(this.value, o.value);
    if (c != 0) return c;
    c = SchemaUtil.compare(this.minLength, o.minLength);
    if (c != 0) return c;
    c = SchemaUtil.compare(this.maxLength, o.maxLength);
    if (c != 0) return c;
    c = SchemaUtil.compare(this.pattern, o.pattern);
    if (c != 0) return c;
    
    return 0;
  } 
}
