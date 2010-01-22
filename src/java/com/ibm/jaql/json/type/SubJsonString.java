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

import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.UndeclaredThrowableException;

/** A JsonString view of a part of a UTF-8 byte array. */  
public final class SubJsonString extends JsonString
{
  int start;
    
  // -- construction ------------------------------------------------------------------------------
  
  public SubJsonString()
  {
    start = 0;
  }
  
  /** Constructs from the provided UTF-8 array without copying. The provided array backs this
   * string. However, changes to the underlying array may or may not be represented by this
   * substring.
   *
   * @param utf8 a byte array containing a UTF-8 encoded string
   * @param start start position
   * @param length number of bytes (not characters!) to use
   */
  public SubJsonString(byte[] utf8, int start, int length)
  {
    set(utf8, start, length);
    hasBytes = true;
  }

  /** Makes sure that the cached Java string is computed from the bytes. */
  @Override
  protected void ensureString() {
    if (cachedString == null)
    {
      assert hasBytes;
      try
      {
        cachedString = new String(bytes, start, bytesLength, "UTF8");
      }
      catch (UnsupportedEncodingException e)
      {
        throw new UndeclaredThrowableException(e);
      }
    }
  }
  
  
  // -- getters -----------------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  public SubJsonString getCopy(JsonValue target) 
  {
    if (this == target) target = null;
    byte[] copy = getCopy();
    
    if (target instanceof SubJsonString)
    {
      SubJsonString s = (SubJsonString)target;
      s.set(copy, 0, bytesLength);
      return s;
    }
    else
    {      
      return new SubJsonString(copy, 0, bytesLength);
    }
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#getImmutableCopy() */
  @Override
  public JsonString getImmutableCopy() 
  {
    return new JsonString(bytes, start, bytesLength);
  }

  /** Retrieves the byte at the specified index. */
  public byte get(int i)
  {
    if (i<0 || i>=bytesLength()) throw new ArrayIndexOutOfBoundsException();
    return bytes[start+i];
  }

  /** Write the internal bytes to the specified buffer. */
  public void writeBytes(int srcpos, byte[] dest, int destpos, int length)
  {
    if (srcpos<0 || srcpos+length>bytesLength()) throw new ArrayIndexOutOfBoundsException();
    System.arraycopy(this.bytes, start+srcpos, dest, destpos, length);
  }

  /** Writes the internal bytes to the specified output. */
  public void writeBytes(DataOutput out) throws IOException
  {
    out.write(this.bytes, start, bytesLength);
  }
  
  @Override
  public int bytesOffset()
  {
    return start;
  }

  // -- mutation ----------------------------------------------------------------------------------
 
  /** Sets from the provided UTF-8 array without copying. The provided array backs this
   * string. However, changes to the underlying array may or may not be represented by this
   * substring.
   *
   * @param utf8 a byte array containing a UTF-8 encoded string
   * @param start start position
   * @param length number of bytes (not characters!) to use
   */
  public void set(byte[] utf8, int start, int length)
  {
    if (start+length > utf8.length) 
    {
      throw new IllegalArgumentException("substring length out of bounds");
    }
    this.bytes = utf8;
    this.start = start;
    this.bytesLength = length;
    invalidateCache();
  }
}
