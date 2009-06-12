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
package com.ibm.jaql.json.type;

import com.ibm.jaql.json.schema.Schema;

/**
 * 
 */
public class JsonSchema extends JsonAtom
{
  protected Schema schema; // This value is shared, so don't mutate it.

  /**
   * 
   */
  public JsonSchema()
  {
  }

  /**
   * @param schema
   */
  public JsonSchema(Schema schema)
  {
    this.schema = schema;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.SCHEMA;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  public int compareTo(Object x)
  {
    throw new RuntimeException("schema are not comparable");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    throw new RuntimeException("schema are not hashable");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JsonValue jvalue) throws Exception
  {
    JsonSchema s = (JsonSchema) jvalue;
    schema = s.schema;
  }

  /**
   * @return
   */
  public Schema getSchema()
  {
    return schema;
  }

  /**
   * @param schema
   */
  public void setSchema(Schema schema)
  {
    this.schema = schema;
  }
}
