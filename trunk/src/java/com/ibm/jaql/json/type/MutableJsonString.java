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


/** A mutable string JSON value. This class stores all strings in UTF-8 encoding. */
public class MutableJsonString extends JsonString
{
  // -- construction ------------------------------------------------------------------------------
  
  /** @see JsonString#JsonString() */
  public MutableJsonString()
  {
    super();
  }

  /** @see JsonString#JsonString(String) */
  public MutableJsonString(String string)
  {
    super(string);
  }

  /** @see JsonString#JsonString(JsonString) */
  public MutableJsonString(JsonString string)
  {
    super(string);
  }

  /** @see JsonString#JsonString(byte[]) */
  public MutableJsonString(byte[] utf8)
  {
    super(utf8);
  }

  /** @see JsonString#JsonString(byte[], int) */
  public MutableJsonString(byte[] utf8, int length)
  {
    super(utf8, length);
  }

  /** @see JsonString#JsonString(byte[], int, int) */
  public MutableJsonString(byte[] utf8, int pos, int length)
  {
    super(utf8, pos, length);
  }

  /** Constructs a MutableJsonString from the provided string. If <code>copy</code> 
   * is set to false, the constructed string will be backed by the same buffer as
   * the provided one. Otherwise, copy construction is performed. */
  public MutableJsonString(MutableJsonString string, boolean copy)
  {
    if (copy)
    {
      setCopy(string);
    }
    else
    {
      set(string.get(), string.bytesLength());
    }
  }
  
  /** Constructs a MutableJsonString from the provided UTF-8 array. If <code>copy</code> 
   * is set to false, the constructed string will be backed by <code>utf8</code>. 
   * Otherwise, a copy of <code>utf8</code> will be created. */
  public MutableJsonString(byte[] utf8, boolean copy)
  {
    this(utf8, utf8.length, copy);
    assert hasBytes;
  }

  /** Constructs a MutableJsonString from the provided UTF-8 array. If <code>copy</code> 
   * is set to false, the constructed string will be backed by <code>utf8</code>. 
   * Otherwise, a copy of <code>utf8</code> will be created. */
  public MutableJsonString(byte[] utf8, int length, boolean copy)
  {
    if (copy)
    {
      setCopy(utf8, length);
    }
    else
    {
      set(utf8, length);
    }
    assert hasBytes;
  }


  // -- getters -----------------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public MutableJsonString getCopy(JsonValue target) 
  {
    if (target == this) target = null;
    
    MutableJsonString t;
    if (target instanceof MutableJsonString)
    {
      t = (MutableJsonString)target;
    }
    else
    {
      t = new MutableJsonString();
    }
    
    t.setCopy(this);
    return t;
  }

  /* @see com.ibm.jaql.json.type.JsonValue#getImmutableCopy() */
  @Override
  public JsonString getImmutableCopy() 
  {
    return new JsonString(this);
  }

  /** Returns the internal byte buffer. Modifications to this buffer will modify the value of
   * this binary. CAUTION: If this buffer is modified, a call to {@link #invalidateCache()}  
   * must be performed!
   */
  public byte[] get()
  {
    ensureBytes();
    return bytes;
  }
  

  // -- mutation ----------------------------------------------------------------------------------

  /** Removes <code>length</code>the bytes starting at <code>offset</code> from the UTF-8 
   * representation of this string. This method will silently ignore invalid arguments. */
  public void removeBytes(int offset, int length)
  {
    ensureBytes();
    final int n = this.bytesLength;
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
      this.bytesLength = offset;
      return;
    }
    for (; i < this.bytesLength; offset++, i++)
    {
      bytes[offset] = bytes[i];
    }
    this.bytesLength -= length;
    invalidateCache();
  }

  /** Find the first occurrence of character <code>oldc</code> by <code>newc</code>. This only 
   * works on 7-bit ascii values right now! */
  public void replace(char oldc, char newc)
  {
    ensureBytes();
    for (int i = 0; i < bytesLength; i++)
    {
      if (bytes[i] == oldc)
      {
        bytes[i] = (byte) newc;
        break;
      }
    }
    invalidateCache();
  }

  // make public
  @Override
  public void setCopy(String string)
  {
    super.setCopy(string);
  }

  // make public
  @Override
  public void setCopy(JsonString string)
  {
    super.setCopy(string);
  }

  // make public
  @Override
  public void set(byte[] utf8)
  {
    super.set(utf8);
  }

  // make public
  @Override
  public void set(byte[] utf8, int length)
  {
    super.set(utf8, length);
  }
  
  // make public
  @Override
  public void setCopy(byte[] utf8) {
    super.setCopy(utf8);
  }

  // make public
  @Override
  public void setCopy(byte[] utf8, int length) {
    super.setCopy(utf8, length);
  }
  
  // make public
  @Override
  public void setCopy(byte[] utf8, int pos, int length) {
    super.setCopy(utf8, pos, length);
  }
  
  // make public
  @Override
  public void ensureCapacity(int capacity)
  {
    super.ensureCapacity(capacity);
  }
  
  // make public
  @Override
  public void invalidateCache()
  {
    super.invalidateCache();
  }  
}
