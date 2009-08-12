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

/** An atomic JSON value representing a byte array.
 * 
 * Instances of this class are immutable, but subclasses might add mutation functionality
 * (in which case they have to override the {@link #getCopy(JsonValue)} method).
 */
public class JsonBinary extends AbstractBinaryJsonAtom
{
  // -- construction ------------------------------------------------------------------------------
  
  /** Copy constructs from the provided byte array. */
  public JsonBinary(byte[] bytes)
  {
    setCopy(bytes);
  }

  /** Copy constructs from the provided byte array.
   * 
   * @param bytes a byte array 
   * @param length number of bytes 
   */
  public JsonBinary(byte[] bytes, int length)
  {
    setCopy(bytes, length);
  }
  
  /** Copy constructs from the provided byte array.
   * 
   * @param bytes a byte array 
   * @param offset starting point of bytes
   * @param length number of bytes 
   */
  public JsonBinary(byte[] bytes, int offset, int length)
  {
    setCopy(bytes, offset, length);
  }

  /** Copy constructs from a hex string, ignoring whitespace. 
   *
   * @throws IllegalArgumentException when the hex string is not valid
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
    bytesLength = n / 2;
    n = 0;
    bytes = new byte[bytesLength];
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
          bytes[n++] = b1;
        }
      }
    }
  }

  
  // -- getters -----------------------------------------------------------------------------------
  
  @Override
  public JsonBinary getCopy(JsonValue target) throws Exception
  {
    return this;
  }
  
  @Override
  public JsonBinary getImmutableCopy() throws Exception
  {
    return this;
  }
  
  // -- comparison/hashing ------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    JsonBinary bi = (JsonBinary) x;
    return WritableComparator.compareBytes(bytes, 0, bytesLength, bi.bytes, 0, bi.bytesLength);
  }

  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode(java.lang.Object) */
  @Override
  public long longHashCode()
  {
    long h = BaseUtil.GOLDEN_RATIO_64;
    for (int i = 0; i < bytesLength; i++)
    {
      h = (h ^ bytes[i]) * BaseUtil.GOLDEN_RATIO_64;
    }
    return h;
  }

  
  // -- misc --------------------------------------------------------------------------------------
  
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.BINARY;
  }
}
