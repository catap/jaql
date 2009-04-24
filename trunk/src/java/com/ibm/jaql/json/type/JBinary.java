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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.WritableComparator;

import com.ibm.jaql.util.BaseUtil;

/** An atomic JSON value representing a byte array. */
public class JBinary extends JAtom
{
  private static final byte[] EMPTY_BUFFER = new byte[0];

  byte[]                value       = EMPTY_BUFFER;
  int                   length;

  /**
   * 
   */
  public JBinary()
  {
    this(EMPTY_BUFFER);
  }

  /** Construct a new JBinary using the given byte array as its value. The array is not copied but
   * directly used as internal buffer. 
   * 
   * @param value a byte array to be used as internal buffer 
   */
  public JBinary(byte[] value)
  {
    this(value, value.length);
  }

  /** Construct a new JBinary using the first <code>length</code> bytes of the given byte array 
   * as its value. The array is not copied but directly used as internal buffer. 
   * 
   * @param value a byte array to be used as internal buffer
   * @param length number of bytes that are valid
   */
  public JBinary(byte[] value, int length)
  {
    this.value = value;
    this.length = length;        
  }

  /** Construct a new JBinary from a hexstring, ignoring whitespace. 
   *
   * @throws IllegalArgumentException when the hexstring is not valid
   */
  public JBinary(String hexString)
  {
    // TODO: two passes seem to be inefficient
    // TODO: first pass can be made more readable using regular expressions
    // TODO: conversion code should be factored to a static utility method
    int n = 0;
    int len = hexString.length();
    for (int i = 0; i < len; i++)
    {
      char c = hexString.charAt(i);
      if ((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z')
          || (c >= 'a' && c <= 'z') || Character.isWhitespace(c))
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
        else
        // if( (c >= 'a' && c <= 'f') )
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

  /** Returns the internal byte[] buffer. It still belongs to this class. */
  public byte[] getBytes()
  {
    return value;
  }

  /** Returns the number of bytes in getBytes() that are valid. */
  public int getLength()
  {
    return length;
  }

  /** Sets the value of this JBinary to the first <code>length</code> bytes of the given byte 
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

  /** Sets the value of this JBinary to the given byte array. The array is not copied but 
   * directly used as internal buffer. 
   * 
   * @param buffer a byte buffer
   */
  public void setBytes(byte[] bytes)
  {
    setBytes(bytes, bytes.length);
  }

  /** Ensures that the internal buffer has at least the provided capacity. The (valid) content of 
   * the byte buffer is retained. 
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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object x)
  {
    JBinary bi = (JBinary) x;
    return WritableComparator.compareBytes(value, 0, length, bi.value, 0, bi.length);
  }

  
  @Override
  public int hashCode() {
    int h = BaseUtil.GOLDEN_RATIO_32;
    for (int i = 0; i < length; i++)
    {
      h = (h ^ value[i]) * BaseUtil.GOLDEN_RATIO_32;
    }
    return h;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    // TODO: need to leave long binaries on disk?
    length = BaseUtil.readVUInt(in);
    value = value.length >= length ? value : new byte[length];
    in.readFully(value, 0, length);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    BaseUtil.writeVUInt(out, length);
    out.write(value, 0, length);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JAtom#print(java.io.PrintStream)
   */
  @Override
  public void print(PrintStream out)
  {
    out.print("x'");
    for (int i = 0; i < length; i++)
    {
      byte b = value[i];
      out.print(BaseUtil.HEX_NIBBLE[(b >> 4) & 0x0f]);
      out.print(BaseUtil.HEX_NIBBLE[b & 0x0f]);
    }
    out.print('\'');
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toJSON()
   */
  @Override
  public String toJSON()
  {
    StringBuffer buf = new StringBuffer();
    buf.append("x'");
    for (int i = 0; i < length; i++)
    {
      byte b = value[i];
      buf.append(BaseUtil.HEX_NIBBLE[(b >> 4) & 0x0f]);
      buf.append(BaseUtil.HEX_NIBBLE[b & 0x0f]);
    }
    buf.append('\'');
    return buf.toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JValue jvalue) throws Exception
  {
    JBinary bi = (JBinary) jvalue;
    setCopy(bi.value, 0, bi.length);
  }
  
  /** Copies data from a byte array into this JBinary. 
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

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.BINARY;
  }
}
