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

/** A string JSON value. This class stores all strings in UTF-8 encoding. 
 * 
 * Instances of this class are immutable, but subclasses might add mutation functionality
 * (in which case they have to override the {@link #getCopy(JsonValue)} method). 
 */
public class JsonString extends AbstractBinaryJsonAtom implements CharSequence
{
  public static final JsonString EMPTY = new JsonString();
  
  // invariant: hasBytes || cachedString != null
  
  /** When true, bytes contains the byte representation of cachedString. Otherwise, cachedString
   * != null and byte conversion is done on demand */
  protected boolean hasBytes;
  
  /** When not null, a String representation of this string */
  protected String              cachedString;
  
  /** When not null, the hash code of this string */
  protected Long                cachedLongHashCode;

  
  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs an empty JSON string. */
  public JsonString()
  {
    super();
    hasBytes = true;
  }

  /** Copy constructs from the provided string. */
  public JsonString(String string)
  {
    setCopy(string);
  }

  /** Copy constructs from the provided <code>JsonString</code>. */
  public JsonString(JsonString string)
  {
    setCopy(string);
  }

  /** Copy constructs from the provided UTF-8 array. */
  public JsonString(byte[] utf8)
  {
    setCopy(utf8);
  }

  /** Copy constructs from the provided UTF-8 array. 
   *
   * @param utf8 a byte array containing a UTF-8 encoded string
   * @param len number of bytes to use
   */
  public JsonString(byte[] utf8, int len)
  {
    setCopy(utf8, len);
  }

  /** Copy constructs from the provided UTF-8 array. 
   *
   * @param utf8 a byte array containing a UTF-8 encoded string
   * @param pos start position of copying
   * @param len number of bytes (not characters!) to use
   */
  public JsonString(byte[] utf8, int pos, int len)
  {
    setCopy(utf8, pos, len);
  }

  /** Makes sure that the bytes are computed from the string. */
  @Override
  protected void ensureBytes()
  {
    if (hasBytes) return;
    assert cachedString != null;
    try 
    {
      bytes = cachedString.getBytes("UTF8");
      this.bytesLength = bytes.length;
      this.hasBytes = true;
    }
    catch (UnsupportedEncodingException e)
    {
      throw new UndeclaredThrowableException(e);
    }    
  }
  
  /** Makes sure that the cached Java string is computed from the bytes. */
  protected void ensureString()
  {
    if (cachedString == null)
    {
      assert hasBytes;
      try
      {
        cachedString = new String(bytes, 0, bytesLength, "UTF8");
      }
      catch (UnsupportedEncodingException e)
      {
        throw new UndeclaredThrowableException(e);
      }
    }
  }
  
  // -- getters -----------------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public JsonString getCopy(JsonValue target) 
  {
    return this;
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#getImmutableCopy() */
  @Override
  public JsonString getImmutableCopy() 
  {
    return this;
  }
  
  /** Returns true if this JSON string starts with the given string. */
  public boolean startsWith(JsonString prefix)
  {
    return toString().startsWith(prefix.toString());
  }

  /** Returns true if this JSON string ends with the given string. */
  public boolean endsWith(JsonString suffix)
  {
    return toString().endsWith(suffix.toString());
  }
  
  /** Find the first occurrence of character <code>c</code>. If found, return the byte index else 
   * return -1. This only works on 7-bit ascii values right now! */
  public int indexOf(char c)
  {
    return toString().indexOf(c);
  }
  
  // -- mutation (all protected) ------------------------------------------------------------------

  /** Sets this <code>JsonString</code> to the provided string. */
  protected void setCopy(String string)
  {
    if (string==null) throw new IllegalArgumentException("string must not be null");
    invalidateCache();
    this.cachedString = string;
    hasBytes = false;
  }
  
  /** Sets this <code>JsonString</code> to the provided string. */
  protected void setCopy(JsonString string)
  {
    if (string.hasBytes)
    {
      this.bytesLength = string.bytesLength;
      ensureCapacity(bytesLength);
      string.writeBytes(this.bytes);
      this.hasBytes = true;
    }
    else
    {
      this.hasBytes = false;
      assert string.cachedString != null;
    }    
    this.cachedString = string.cachedString;
    this.cachedLongHashCode = string.cachedLongHashCode;
  }

  // clear cache when setting value
  @Override
  protected void set(byte[] utf8, int length)
  {
    super.set(utf8, length);
    hasBytes = true;
    invalidateCache();
  }
  
  // clear cache when setting value
  @Override
  protected void setCopy(byte[] utf8, int offset, int length)
  {
    super.setCopy(utf8, offset, length);
    hasBytes = true;
    invalidateCache();
  }

  // -- comparison/hashing ------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    JsonString s = (JsonString) x;
    ensureBytes();
    s.ensureBytes();
    int len = Math.min(this.bytesLength(), s.bytesLength());
    for (int i = 0; i < len; i++)
    {
      int c = (int) (this.get(i) & 0xff) - (int) (s.get(i) & 0xff);
      if (c != 0)
      {
        return c;
      }
    }
    int c = this.bytesLength() - s.bytesLength();
    return c;
  }
  
  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    if (cachedLongHashCode != null) return cachedLongHashCode;

    ensureBytes();
    int n = bytesLength();
    long h = BaseUtil.GOLDEN_RATIO_64;
    for (int i = 0; i < n; i++)
    {
      h ^= get(i);
      h *= BaseUtil.GOLDEN_RATIO_64;
    }
    cachedLongHashCode = h; // remember it
    return h;
  }

  protected void invalidateCache()
  {
    this.cachedString = null;
    this.cachedLongHashCode = null;
  }  

  // -- misc --------------------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.STRING;
  }

  // -- CharSequence ------------------------------------------------------------------------------
  

  
  /** Converts this string to a Java string. This method internally uses caching; the conversion
   * cost is thus paid only once. */
  @Override
  public String toString()
  {
    ensureString();
    return cachedString;
  }
  
  /** Returns the number of characters in this string */
  @Override
  public int length()
  {
    return toString().length();
  }
  
  @Override
  public char charAt(int index)
  {
    return toString().charAt(index);
  }

  @Override
  public CharSequence subSequence(int start, int end)
  {
    return toString().subSequence(start, end);
  }
}
