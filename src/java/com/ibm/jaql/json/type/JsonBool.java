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

/** A boolean JSON value. */
public class JsonBool extends JsonAtom
{
  // TODO: should be immutable
  public final static JsonBool TRUE  = new JsonBool(true);
  public final static JsonBool FALSE = new JsonBool(false);

  protected boolean value;

  
  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs a new JsonBool with an undefined value. */
  public JsonBool()
  {
  }

  /** Constructs a new JsonBool representing the given value */
  public JsonBool(boolean value)
  {
    this.value = value;
  }

  /** Returns a {@link JsonBool} for the given value. The returned value must not be changed;
   * is mutation is required, use one of the constructors instead. */
  public static JsonBool makeShared(boolean value)
  {
    return value ? TRUE : FALSE;
  }

  
  // -- getters -----------------------------------------------------------------------------------
  
  /** Returns the boolean value */
  public boolean get()
  {
    return value;
  }

  /* @see JsonValue#getCopy(JsonValue) */
  @Override
  public JsonBool getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    if (target instanceof JsonBool)
    {
      JsonBool t = (JsonBool)target;
      t.value = value;
      return t;
    }
    return new JsonBool(value);
  }


  // -- setters -----------------------------------------------------------------------------------
  
  /** Sets the boolean value */
  public void set(boolean value)
  {
    this.value = value;
  }

  
  // -- comparison/hashing ------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  public int compareTo(Object x)
  {
    //    int c = Util.typeCompare(this, (Writable)x);
    //    if( c != 0 )
    //    {
    //      return c;
    //    }
    boolean value2 = ((JsonBool) x).value;
    return (value == value2) ? 0 : (value2 ? -1 : +1);
  }

  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    return value ? JsonLong.longHashCode(1) : JsonLong.longHashCode(0);
  }

  
  // -- misc --------------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.BOOLEAN;
  }


}
