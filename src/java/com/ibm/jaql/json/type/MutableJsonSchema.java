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
import com.ibm.jaql.json.schema.SchemaFactory;

/** A mutable JSON value that stores a JSON schema. */
public class MutableJsonSchema extends JsonSchema
{
  // -- construction ------------------------------------------------------------------------------

  /** Constructs a new <code>MutableJsonSchema</code> representing any schema. */
  public MutableJsonSchema()
  {
    super(SchemaFactory.anySchema());
  }

  /** @see JsonSchema#JsonSchema(Schema) */
  public MutableJsonSchema(Schema schema)
  {
    super(schema);
  }


  // -- getters -----------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public MutableJsonSchema getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    if (target instanceof MutableJsonSchema)
    {
      MutableJsonSchema t = (MutableJsonSchema)target;
      t.schema = this.schema; // immutable
      return t;
    }
    return new MutableJsonSchema(schema);
  }

  /* @see com.ibm.jaql.json.type.JsonValue#getImmutableCopy() */
  @Override
  public JsonSchema getImmutableCopy() 
  {
    return new JsonSchema(this);
  }

  // -- mutation ----------------------------------------------------------------------------------

  /** Sets the schema represented by this value. */
  public void set(Schema schema)
  {
    if (schema == null) throw new IllegalArgumentException("schema must not be null");
    this.schema = schema;
  }
}
