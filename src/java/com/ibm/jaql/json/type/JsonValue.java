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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.io.serialization.text.TextFullSerializer;

/** A JSON value. */
public abstract class JsonValue implements Comparable<Object>
{
  // -- getters -----------------------------------------------------------------------------------
  
  /** Obtain a copy of this value. If this value is immutable, returns itself. If this value
   * is mutable, the method tries to copy it into the specified target, if possible. Otherwise, 
   * returns a fresh copy of this value. */
  public abstract JsonValue getCopy(JsonValue target) throws Exception;

  /** Returns an immutable copy of this value. This method should be preferred over
   * {@link #getCopy(JsonValue)} whenever no target is available and mutability is not required. */
  public abstract JsonValue getImmutableCopy() throws Exception;
  /**
   * Convert this value to a Java String. The default is the JSON string, but
   * some classes will override to return other strings.
   */
  @Override
  public String toString() 
  {
    try
    {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(bout, false, "UTF-8");
      TextFullSerializer.getDefault().write(out, this);    
      return bout.toString("UTF-8");
    } 
    catch (IOException e)
    {
      // TODO: print exception?
      throw new UndeclaredThrowableException(e);
    }
  }

  
  // -- comparison/hashing ------------------------------------------------------------------------

  @Override
  public boolean equals(Object x)
  {
    if (x instanceof JsonValue)
    {
      return this.compareTo((JsonValue)x) == 0;
    }
    return false;
  }

  @Override
  public final int hashCode()
  {
    long h = longHashCode();
    return (int) (h ^ (h >>> 32));
  }
  
  /** Returns a long hash code for this value. */
  public abstract long longHashCode();
  

  // -- misc --------------------------------------------------------------------------------------

  /** Returns the encoding of this object. */
  public abstract JsonEncoding getEncoding();
  
  /** Returns the type of this object. */
  public JsonType getType()
  {
    return getEncoding().getType();
  }
}
