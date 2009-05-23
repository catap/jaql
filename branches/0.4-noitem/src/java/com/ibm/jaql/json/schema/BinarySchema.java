package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;

public class BinarySchema extends Schema
{
  private JsonLong minLength;
  private JsonLong maxLength;
  
  public BinarySchema(JsonLong minLength, JsonLong maxLength)
  {
    // check arguments
    if (!SchemaUtil.checkInterval(minLength, maxLength, JsonLong.ZERO, JsonLong.ZERO))
    {
      throw new IllegalArgumentException("lengths out of bounds: " + minLength + " " + maxLength);
    }

    // store length
    if (minLength != null || maxLength != null)
    {
      this.minLength = minLength==null ? JsonLong.ZERO : minLength;
      this.maxLength = maxLength;
    }
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
  

  @Override
  public SchemaType getSchemaType()
  {
    return SchemaType.BINARY;
  }
}
