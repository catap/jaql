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
package com.ibm.jaql.lang.expr.function;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;

/** A parameter that takes a JSON value as default value. */
public class JsonValueParameter extends Parameter<JsonValue>
{
  public JsonValueParameter(JsonString name, Schema schema, boolean isRepeating)
  {
    super(name, schema, isRepeating);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameter(JsonString name, Schema schema,
      JsonValue defaultValue)
  {
    super(name, schema, defaultValue);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameter(JsonString name, Schema schema)
  {
    super(name, schema);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameter(JsonString name, String schema,
      JsonValue defaultValue)
  {
    super(name, schema, defaultValue);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameter(JsonString name, String schema)
  {
    super(name, schema);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameter(JsonString name)
  {
    super(name);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameter(String name, Schema schema, boolean isRepeating)
  {
    super(name, schema, isRepeating);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameter(String name, Schema schema, JsonValue defaultValue)
  {
    super(name, schema, defaultValue);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameter(String name, Schema schema)
  {
    super(name, schema);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameter(String name, String schema, JsonValue defaultValue)
  {
    super(name, schema, defaultValue);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameter(String name, String schema)
  {
    super(name, schema);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameter(String name)
  {
    super(name);
    // TODO Auto-generated constructor stub
  }

  @Override
  protected JsonValue processDefault(JsonValue value)
  {
     JsonValue v = JsonUtil.getImmutableCopyUnchecked(value);
     if (!(getSchema().matchesUnsafe(v))) 
       throw new IllegalArgumentException("default value does not match provided schema");
     return v;
  }
  

}
