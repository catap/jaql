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

/** A mutable boolean JSON value. */
public class MutableJsonBool extends JsonBool
{
  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs a new MutableJsonBool representing <code>false</code>. */
  public MutableJsonBool()
  {
    super(false);
  }

  /** @see JsonBool#JsonBool(boolean) */
  public MutableJsonBool(boolean value)
  {
    super(value);
  }

  
  // -- getters -----------------------------------------------------------------------------------
  
  /* @see JsonValue#getCopy(JsonValue) */
  @Override
  public MutableJsonBool getCopy(JsonValue target) throws Exception
  {
    if (target instanceof MutableJsonBool)
    {
      MutableJsonBool t = (MutableJsonBool)target;
      t.value = value;
      return t;
    }
    return new MutableJsonBool(value);
  }

  /* @see com.ibm.jaql.json.type.JsonValue#getImmutableCopy() */
  @Override
  public JsonBool getImmutableCopy() 
  {
    return JsonBool.make(value);
  }

  // -- setters -----------------------------------------------------------------------------------
  
  /** Sets the boolean value */
  public void set(boolean value)
  {
    this.value = value;
  }
}
