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

/** An boolean JSON value.
 * 
 * Instances of this class are immutable, but subclasses might add mutation functionality
 * (in which case they have to override the {@link #getCopy(JsonValue)} method).
 */
public class JsonBool extends JsonAtom
{
  public final static JsonBool TRUE  = new JsonBool(true);
  public final static JsonBool FALSE = new JsonBool(false);

  protected boolean value = false;


  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs a new JsonBool representing the given value */
  protected JsonBool(boolean value)
  {
    this.value = value;
  }

  /** Returns an (immutable) {@link JsonBool} for the given value. */
  public static JsonBool make(boolean value)
  {
    return value ? TRUE : FALSE;
  }

  public static JsonBool make(String string)
  {
    return make(Boolean.parseBoolean(string));
  }

  public static JsonBool make(JsonString string)
  {
    return make(Boolean.parseBoolean(string.toString()));
  }

  // -- getters -----------------------------------------------------------------------------------
  
  /** Returns the boolean value */
  public boolean get()
  {
    return value;
  }

  @Override
  public JsonBool getCopy(JsonValue target) throws Exception
  {
    return this;  // immutable, no copying needed
  }
  
  @Override
  public JsonBool getImmutableCopy() throws Exception
  {
    return this;
  }

  
  // -- comparison/hashing ------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  public int compareTo(Object x)
  {
    boolean value2 = ((JsonBool) x).get();
    return (get()== value2) ? 0 : (value2 ? -1 : +1);
  }

  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    return get() ? JsonLong.longHashCode(1) : JsonLong.longHashCode(0);
  }


  // -- misc --------------------------------------------------------------------------------------

  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.BOOLEAN;
  }
}
