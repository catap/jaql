package com.ibm.jaql.json.schema;

import java.util.regex.Pattern;

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class StringSchema extends Schema
{
  private JsonString pattern;
  private Pattern compiledPattern;
  private JsonLong minLength;
  private JsonLong maxLength;
  
  // TODO: format property
  
  public StringSchema() { }
  
  public StringSchema(JsonString pattern, JsonLong minLength, JsonLong maxLength)
  {
    // check arguments
    if (!SchemaUtil.checkInterval(minLength, maxLength, JsonLong.ZERO, JsonLong.ZERO))
    {
      throw new IllegalArgumentException("string lengths out of bounds: " + minLength + " " + maxLength);
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
  }
  
  public StringSchema(JsonString pattern)
  {
    this(pattern, null, null);
  }
  
  public StringSchema(JsonLong minLength, JsonLong maxLength)
  {
    this(null, minLength, maxLength);    
  }
  
  @Override
  public boolean matches(JsonValue value) throws Exception
  {
    if (!(value instanceof JsonString))
    {
      return false;
    }
    JsonString s = (JsonString)value;

    // check string length
    // TODO: currently uses UTF8 representation
    if (!(minLength==null || s.getLength()>=minLength.value)) return false;
    if (!(maxLength==null || s.getLength()<=maxLength.value)) return false;

    // check regexp pattern
    if (!(pattern == null || compiledPattern.matcher(s.toString()).matches())) return false;

    // everythink ok
    return true;
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

  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.STRING;
  }
}
