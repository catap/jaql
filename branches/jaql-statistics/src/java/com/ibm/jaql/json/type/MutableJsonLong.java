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


/** A mutable long JSON value (64-bit signed integer). */
public class MutableJsonLong extends JsonLong
{

  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs a new <code>MutableJsonLong</code> having value 0. */
  public MutableJsonLong()
  {
    super(0);
  }

  /** @see JsonLong#JsonLong(long) */
  public MutableJsonLong(long value)
  {
    super(value);
  }

  /** @see JsonLong#JsonLong(JsonLong) */
  public MutableJsonLong(JsonLong value)
  {
    super(value);
  }
  
  /** @see JsonLong#JsonLong(String) */
  public MutableJsonLong(String value) throws NumberFormatException
  {
    super(value);
  }

  
  // -- getters -----------------------------------------------------------------------------------

  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public MutableJsonLong getCopy(JsonValue target)
  {
    if (target == this) target = null;
    
    if (target instanceof MutableJsonLong)
    {
      MutableJsonLong t = (MutableJsonLong)target;
      t.value = this.value;
      return t;
    }
    return new MutableJsonLong(value);
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#getImmutableCopy() */
  @Override
  public JsonLong getImmutableCopy() 
  {
    return JsonLong.make(value);
  }
  
  // -- setters -----------------------------------------------------------------------------------

  /** Sets the value of this <code>JsonLong</code> to the specified value. */
  public void set(long value)
  {
    this.value = value;
  }
  

  /** Negates this JsonLong. This method does not produce meaningful results when it represents
   * {@link Long#MAX_VALUE}. */
  public void negate()
  {
    value = -value;
  }
}
