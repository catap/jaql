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

/** A JSON value that stores a JSON schema. 
 * 
 * Instances of this class are immutable, but subclasses might add mutation functionality
 * (in which case they have to override the {@link #getCopy(JsonValue)} method). 
 */
public class JsonSchema extends JsonAtom
{
  protected Schema schema; 

  // -- construction ------------------------------------------------------------------------------

  /** Constructs a new <code>JsonSchema</code> representing the specified schema. */
  public JsonSchema(Schema schema)
  {
    if (schema == null) throw new IllegalArgumentException("schema must not be null");
    this.schema = schema; // Schema is immutable
  }

  /** Copy constructs from the specified value. */
  public JsonSchema(JsonSchema schema)
  {
    this(schema.schema);
  }

  // -- getters -----------------------------------------------------------------------------------

  /** Returns the schema represented by this value. */
  public Schema get()
  {
    return schema;
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public JsonSchema getCopy(JsonValue target) throws Exception
  {
    return this; // immutable
  }

  @Override
  public JsonSchema getImmutableCopy() throws Exception
  {
    return this;
  }
  
  // -- comparison/hashing ------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  public int compareTo(Object x)
  {
    JsonSchema o = (JsonSchema)x;
    return this.schema.compareTo(o.schema);
  }

  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    throw new RuntimeException("schemata are not hashable");
  }
  
  
  // -- misc --------------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.SCHEMA;
  }
}
