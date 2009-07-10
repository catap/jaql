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

/** Superclass for all JSON values. Provides abstract methods for serialization, conversion 
 * to JSON language, deep copying, and hashing. See {@link JsonUtil} for useful utility methods
 * in JSON values. */
public abstract class JsonValue implements Comparable<Object>
{

  // -- getters -----------------------------------------------------------------------------------
  
  /** Obtain a copy of this value. Reuses the specified target, if possible. Otherwise, returns
   * a fresh copy of this value */
  public abstract JsonValue getCopy(JsonValue target) throws Exception;

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
      PrintStream out = new PrintStream(bout);
      TextFullSerializer.getDefault().write(out, this);    
      return bout.toString();
    } 
    catch (IOException e)
    {
      // TODO: print exception?
      throw new UndeclaredThrowableException(e);
    }
  }

  
  // -- comparison/hashing ------------------------------------------------------------------------

  /* @see java.lang.Object#equals(java.lang.Object) */
  @Override
  public boolean equals(Object x)
  {
    return this.compareTo(x) == 0;
  }

  /* @see java.lang.Comparable#compareTo(java.lang.Object) */
  public abstract int compareTo(Object obj);

  /* @see java.lang.Object#hashCode() */
  @Override
  public int hashCode()
  {
    return (int) (longHashCode() >>> 32);
  }
  
  /** Returns a long hash code for this value.
   * 
   * @return a long hash code
   */
  public abstract long longHashCode();
  

  // -- misc --------------------------------------------------------------------------------------

  /** Returns the encoding of this object.
   * 
   * @return the encoding of this object
   */
  public abstract JsonEncoding getEncoding();  
}
