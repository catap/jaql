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



/** A mutable double JSON value (64-bit base-2 floating point value). */
public class MutableJsonDouble extends JsonDouble
{
  // -- construction ------------------------------------------------------------------------------

  /** Constructs a new <code>MutableJsonDouble</code> having value 0. */
  public MutableJsonDouble()
  {
    super(0);
  }

  /** @see JsonDouble#JsonDouble(double) */
  public MutableJsonDouble(double value)
  {
    super(value);
  }
  
  /** @see JsonDouble#JsonDouble(JsonDouble) */
  public MutableJsonDouble(JsonDouble value)
  {
    super(value);
  }

  /** @see JsonDouble#JsonDouble(String) */
  public MutableJsonDouble(String value) throws NumberFormatException
  {
    super(value);
  }

  // -- getters -----------------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public MutableJsonDouble getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    if (target instanceof MutableJsonDouble)
    {
      MutableJsonDouble t = (MutableJsonDouble)target;
      t.value = this.value;
      return t;
    }
    return new MutableJsonDouble(value);
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#getImmutableCopy() */
  @Override
  public JsonDouble getImmutableCopy() 
  {
    return new JsonDouble(this);
  }
  
  // -- mutation ----------------------------------------------------------------------------------

  /** Negates this value */ 
  public void negate()
  {
    value = -value;
  }
  
  /** Sets the value of this <code>JsonDouble</code> to the specified value. */
  public void set(double value)
  {
    this.value = value;
  }
}
