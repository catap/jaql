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

import org.apache.hadoop.io.WritableComparator;

import com.ibm.jaql.util.BaseUtil;

/** An atomic JSON value representing a byte array. */
public class JsonBinary extends JsonAtom
{
  protected static final byte[] EMPTY_BUFFER = new byte[0];

  protected byte[] value = EMPTY_BUFFER;
  protected int length;


  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs an emtpy byte array. */
  public JsonBinary()
  {
    this(EMPTY_BUFFER);
  }
  
  /** Construct a new JsonBinary using the given byte array as its value. The array is not copied but
   * directly used as internal buffer. 
   * 
   * @param value a byte array to be used as internal buffer 
   */
  public JsonBinary(byte[] value)
  {
    this(value, value.length);
  }

  /** Construct a new JsonBinary using the first <code>length</code> bytes of the given byte array 
   * as its value. The array is not copied but directly used as internal buffer. 
   * 
   * @param value a byte array to be used as internal buffer
   * @param length number of bytes that are valid
   */
  public JsonBinary(byte[] value, int length)
  {
    this.value = value;
    this.length = length;        
  }

  /** Construct a new JsonBinary from a hexstring, ignoring whitespace. 
   *
   * @throws IllegalArgumentException when the hexstring is not valid
   */
  public JsonBinary(String hexString)
  {
    // TODO: two passes seem to be inefficient
    // TODO: first pass can be made more readable using regular expressions
    // TODO: conversion code should be factored to a static utility method
    int n = 0;
    int len = hexString.length();
    for (int i = 0; i < len; i++)
    {
      char c = hexString.charAt(i);
      if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')
          || (c >= 'a' && c <= 'f') || Character.isWhitespace(c))
      {
        n++;
      }
      else
      {
        throw new IllegalArgumentException("bad hex character: " + c);
      }
    }
    if ((n & 0x01) != 0)
    {
      throw new IllegalArgumentException("hex string must be a multiple of two");
    }
    length = n / 2;
    n = 0;
    value = new byte[length];
    byte b1 = 0;
    boolean half = false;
    for (int i = 0; i < len; i++)
    {
      char c = hexString.charAt(i);
      if (!Character.isWhitespace(c))
      {
        byte b2;
        if ((c >= '0' && c <= '9'))
        {
          b2 = (byte) (c - '0');
        }
        else if ((c >= 'A' && c <= 'F'))
        {
          b2 = (byte) (c - 'A' + 10);
        }
        else // if( (c >= 'a' && c <= 'f') )
        {
          b2 = (byte) (c - 'a' + 10);
        }
        half = !half;
        if (half)
        {
          b1 = b2;
        }
        else
        {
          b1 = (byte) ((b1 << 4) | b2);
          value[n++] = b1;
        }
      }
    }
  }

  
  // -- reading/writing ---------------------------------------------------------------------------

  /** Returns the internal byte buffer. Use with caution! */
  public byte[] getInternalBytes()
  {
    return value;
  }

  /** Returns the number of bytes in {@link #getBytes()} that are valid. */
  public int length()
  {
    return length;
  }



  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public JsonBinary getCopy(JsonValue target) throws Exception
  {
    if (target == this) target = null;
    
    JsonBinary t;
    if (target instanceof JsonBinary)
    {
      t = (JsonBinary)target;
    }
    else
    {
      t = new JsonBinary();
    }
    t.setCopy(this.value, 0, this.length);
    return t;
  }
  
  
  // -- mutation ----------------------------------------------------------------------------------
  
  /** Sets the value of this JsonBinary to the first <code>length</code> bytes of the given byte 
   * array. The array is not copied but directly used as internal buffer. 
   * 
   * @param buffer a byte buffer
   * @param length number of bytes that are valid
   */
  public void setBytes(byte[] bytes, int length)
  {
    this.value = bytes;
    this.length = length;
  }

  /** Sets the value of this JsonBinary to the given byte array. The array is not copied but 
   * directly used as internal buffer. 
   * 
   * @param buffer a byte buffer
   */
  public void setBytes(byte[] bytes)
  {
    setBytes(bytes, bytes.length);
  }

  /** Ensures that the internal buffer has at least the provided capacity but neither changes
   * nor increases the valid bytes (the first {@link #size()} bytes).  
   * 
   * @param capacity
   */
  public void ensureCapacity(int capacity)
  {
    if (capacity > value.length)
    {
      byte[] newval = new byte[capacity];
      System.arraycopy(value, 0, newval, 0, length); // non-valid bytes are not copied
      value = newval;
    }
  }
  
  /** Copies data from a byte array into this JsonBinary. 
   * 
   * @param buf a byte array
   * @param pos position in byte array
   * @param length number of bytes to copy
   */ 
  public void setCopy(byte[] buf, int pos, int length) {
    this.length = length;
    value = value.length >= length ? value : new byte[length];
    System.arraycopy(buf, pos, value, 0, length);
  }
  
  
  // -- comparison/hashing ------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    JsonBinary bi = (JsonBinary) x;
    return WritableComparator.compareBytes(value, 0, length, bi.value, 0, bi.length);
  }

  /* @see com.ibm.jaql.json.type.JsonValue#hashCode(java.lang.Object) */
  @Override
  public int hashCode() {
    int h = BaseUtil.GOLDEN_RATIO_32;
    for (int i = 0; i < length; i++)
    {
      h = (h ^ value[i]) * BaseUtil.GOLDEN_RATIO_32;
    }
    return h;
  }

  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode(java.lang.Object) */
  @Override
  public long longHashCode()
  {
    long h = BaseUtil.GOLDEN_RATIO_64;
    for (int i = 0; i < length; i++)
    {
      h = (h ^ value[i]) * BaseUtil.GOLDEN_RATIO_64;
    }
    return h;
  }



  
  // -- misc --------------------------------------------------------------------------------------
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.BINARY;
  }
  
  
  
}
