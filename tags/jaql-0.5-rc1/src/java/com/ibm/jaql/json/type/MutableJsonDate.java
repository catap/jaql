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

import java.text.DateFormat;

/** A mutable JSON date. */
public class MutableJsonDate extends JsonDate
{
  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs a new <code>MutableJsonDate</code> representing January 1, 1970, 00:00:00 GMT. */
  public MutableJsonDate()
  {
    super(0);
  }

  /** @see JsonDate#JsonDate(long) */
  public MutableJsonDate(long millis)
  {
    super(millis);
  }

  /** @see JsonDate#JsonDate(String,DateFormat) */
  public MutableJsonDate(String date, DateFormat format)
  {
    super(date, format);
  }

  /** @see JsonDate#JsonDate(String) */
  public MutableJsonDate(String date)
  {
    super(date);
  }
  
  /** @see JsonDate#JsonDate(JsonString) */
  public MutableJsonDate(JsonString date)
  {
    super(date);
  }
  
  /** @see JsonDate#JsonDate(String,String) */
  public MutableJsonDate(String date, String format)
  {
    super(date, format);
  }

  /** @see JsonDate#JsonDate(JsonString,JsonString) */
  public MutableJsonDate(JsonString date, JsonString format)
  {
    super(date, format);
  }
  

  // -- getters -----------------------------------------------------------------------------------

  @Override
  public MutableJsonDate getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    if (target instanceof MutableJsonDate)
    {
      MutableJsonDate t = (MutableJsonDate)target;
      t.millis = this.millis;
      return t;
    }
    return new MutableJsonDate(millis);
  }
  
  @Override
  public JsonDate getImmutableCopy() 
  {
    return new JsonDate(millis);
  }
  
  // -- setters -----------------------------------------------------------------------------------

  // increase visibility
  public void set(long millis)
  {
    super.set(millis);
  }

  // increase visibility 
  public void set(String date, DateFormat format)
  {
    super.set(date, format);
  }
  
  // increase visibility
  public void set(String date)
  {
    super.set(date);
  }
}
