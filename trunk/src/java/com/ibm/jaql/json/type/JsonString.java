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

import java.io.UnsupportedEncodingException;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.util.BaseUtil;

/** A string JSON value. This class stores all strings in UTF-8 encoding. */
public class JsonString extends JsonAtom
{
  protected final static byte[] NO_BYTES = new byte[0];
  
  protected byte[]              bytes    = NO_BYTES;
  protected int                 length = 0;
  protected String              cachedString = null;
  protected Long                cachedLongHashCode = null;

  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs an empty <code>JsonString</code>. */
  public JsonString()
  {
  }

  /** Constructs a <code>JsonString</code> representing the provided string. 
   * 
   * @see #set(String) 
   */
  public JsonString(String string)
  {
    set(string);
  }

  /** Constructs a copy of the provided <code>JsonString</code>. */
  public JsonString(JsonString string)
  {
    setCopy(string.bytes, string.length);
    this.cachedString = string.cachedString;
    this.cachedLongHashCode = string.cachedLongHashCode;
  }

  /** Constructs a <code>JsonString</code> representing the specified value without copying. 
   * A reference to the provided byte array is stored within the constructed instance.    
   * 
   * @param utf8 a byte array containing a UTF-8 encoded string 
   */
  public JsonString(byte[] utf8)
  {
    set(utf8, utf8.length);
  }

  /** Constructs a <code>JsonString</code> representing the specified value without copying. 
   * A reference to the provided byte array is stored within the constructed instance.    
   *
   * @param utf8 a byte array containing a UTF-8 encoded string
   * @param len number of bytes (not characters!) to use
   */
  public JsonString(byte[] utf8, int len)
  {
    set(utf8, len);
  }


  // -- getters -----------------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public JsonString getCopy(JsonValue target) 
  {
    if (target == this) target = null;
    
    JsonString t;
    if (target instanceof JsonString)
    {
      t = (JsonString)target;
    }
    else
    {
      t = new JsonString();
    }
    
    t.setCopy(bytes, length);
    t.cachedString = cachedString;
    t.cachedLongHashCode = cachedLongHashCode;
    return t;
  }
  
  /** Converts this string to a Java string. This method internally uses caching; the conversion
   * cost is thus paid only once. */
  @Override
  public String toString()
  {
    try
    {
      if (cachedString == null)
      {
        cachedString = new String(bytes, 0, length, "UTF8");
      }
      return cachedString;
    }
    catch (UnsupportedEncodingException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }
  
  /** Returns the number of UTF-8 bytes (not characters!). */
  public int lengthUtf8()
  {
    return length;
  }

  /** Returns true if this JSON string starts with the given string. This method currently compares
   * the UTF-8 codes (not characters) of both strings. */
  public boolean startsWith(JsonString prefix)
  {
    // TODO: this does UTF-8 comparision; not character comparison
    int n = prefix.length;
    if (length < n)
    {
      return false;
    }
    byte[] t = bytes;
    byte[] p = prefix.bytes;
    for (int i = 0; i < n; i++)
    {
      if (t[i] != p[i])
      {
        return false;
      }
    }
    return true;
  }

  /** Returns true if this JSON string ends with the given string. This method currently compares
   * the UTF-8 codes (not characters) of both strings. */
  public boolean endsWith(JsonString suffix)
  {
    // TODO: this does UTF-8 comparision; not character comparison
    int n = suffix.length;
    if (length < n)
    {
      return false;
    }
    byte[] myBytes = bytes;
    byte[] suffixBytes = suffix.bytes;
    for (int i = 0; i < n; i++)
    {
      if (myBytes[length-n+i] != suffixBytes[i])
      {
        return false;
      }
    }
    return true;
  }
  
  /** Returns the internal byte buffer that backs this JSON string. The bytes should not be modified
   * (or methods like {@link toString()} or {@link #hashCode()} might not work). */
  public byte[] getInternalBytes()
  {
    return bytes;
  }

  /** Find the first occurence of character <code>c</code>. If found, return the byte index else 
   * return -1. This only works on 7-bit ascii values right now! */
  public int indexOf(char c)
  {
    for (int i = 0; i < length; i++)
    {
      if (bytes[i] == c)
      {
        return i;
      }
    }
    return -1;
  }
  
  /** Removes <code>length</code>the bytes starting at <code>offset</code> from the UTF-8 
   * representation of this string. This method will silently ignore invalid arguments. */
  public void removeBytes(int offset, int length)
  {
    final int n = this.length;
    if (offset < 0 || offset >= n || length <= 0)
    {
      // out of range
      return;
    }
    int i = offset + length;
    assert i>=0;
    if (i >= n)
    {
      // removing the tail
      this.length = offset;
      return;
    }
    for (; i < this.length; offset++, i++)
    {
      bytes[offset] = bytes[i];
    }
    this.length -= length;
    clearCache();
  }

  /** Find the first occurence of character <code>oldc</code> by <code>newc</code>. This only 
   * works on 7-bit ascii values right now! */
  public void replace(char oldc, char newc)
  {
    for (int i = 0; i < length; i++)
    {
      if (bytes[i] == oldc)
      {
        bytes[i] = (byte) newc;
        break;
      }
    }
  }

  // -- mutation ----------------------------------------------------------------------------------

  /** Sets this <code>JsonString</code> to the provided string. */
  public void set(String string)
  {
    try
    {
      clearCache();
      this.cachedString = string;
      bytes = string.getBytes("UTF8");
      this.length = bytes.length;
    }
    catch (UnsupportedEncodingException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }
  
  /** Sets this <code>JsonString</code> to the specified value without copying.
   * A reference to the provided byte array is stored within this <code>JsonString</code>.    
   * 
   * @param utf8 a byte array containing a UTF-8 encoded string
   */
  public void set(byte[] utf8)
  {
    set(utf8, utf8.length);
  }

  /** Sets this <code>JsonString</code> to the specified value without copying.
   * A reference to the provided byte array is stored within this <code>JsonString</code>.    
   *
   * @param utf8 a byte array containing a UTF-8 encoded string
   * @param length number of bytes (not characters!) to use
   */
  public void set(byte[] utf8, int length)
  {
    this.bytes = utf8;
    this.length = length;
    clearCache();
  }


  /** Sets this <code>JsonString</code> to the specified value with copying.
  *
  * @param utf8 a byte array containing a UTF-8 encoded string
  */
  public void setCopy(byte[] utf8)
  {
    setCopy(utf8, 0, utf8.length);
  }

  /** Sets this <code>JsonString</code> to the specified value with copying.
  *
  * @param utf8 a byte array containing a UTF-8 encoded string
  * @param length number of bytes (not characters!) to use
  */
  public void setCopy(byte[] utf8, int length)
  {
    setCopy(utf8, 0, length);
  }

  /** Sets this <code>JsonString</code> to the specified value with copying.
   *
   * @param utf8 a byte array containing a UTF-8 encoded string
   * @param offset in <code>utf8</code> at which to start copying
   * @param length number of bytes (not characters!) to use
   */
  public void setCopy(byte[] utf8, int offset, int length)
  {
    if (bytes.length < length)
    {
      bytes = new byte[length];
    }
    System.arraycopy(utf8, offset, bytes, 0, length);
    this.length = length;
    clearCache();
  }
  

  // -- comparison/hashing ------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    JsonString s = (JsonString) x;
    int len = Math.min(this.length, s.length);
    for (int i = 0; i < len; i++)
    {
      int c = (int) (bytes[i] & 0xff) - (int) (s.bytes[i] & 0xff);
      if (c != 0)
      {
        return c;
      }
    }
    int c = this.length - s.length;
    return c;
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    if (cachedLongHashCode != null) return cachedLongHashCode;

    byte[] bs = bytes;
    int n = length;
    long h = BaseUtil.GOLDEN_RATIO_64;
    for (int i = 0; i < n; i++)
    {
      h ^= bs[i];
      h *= BaseUtil.GOLDEN_RATIO_64;
    }
    cachedLongHashCode = h; // remember it
    return h;
  }


  // -- misc --------------------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.STRING;
  }

  protected void clearCache()
  {
    this.cachedString = null;
    this.cachedLongHashCode = null;
  }  
}
