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

import com.ibm.jaql.util.BaseUtil;

/**
 * 
 */
public class JBinary extends JAtom
{
  private static byte[] emptyBuffer = new byte[0];

  byte[]                value       = emptyBuffer;
  int                   length;

  /**
   * 
   */
  public JBinary()
  {
  }

  /**
   * @param value
   */
  public JBinary(byte[] value)
  {
    this.value = value;
    this.length = value.length;
  }

  /**
   * @param value
   * @param length
   */
  public JBinary(byte[] value, int length)
  {
    this.value = value;
    this.length = length;
  }

  /**
   * @param hexString
   */
  public JBinary(String hexString)
  {
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
        throw new RuntimeException("bad hex character: " + c);
      }
    }
    if ((n & 0x01) != 0)
    {
      throw new RuntimeException("hex string must be a multiple of two");
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

  /**
   * 
   * @return The current byte[] buffer. It still belongs to this class.
   */
  public byte[] getBytes()
  {
    return value;
  }

  /**
   * 
   * @return The number of bytes in getBytes() that are valid.
   */
  public int getLength()
  {
    return length;
  }

  /**
   * 
   * @param buffer
   *            The buffer now belongs to this class
   * @param length
   *            The number of bytes in the buffer that are part of the current
   *            value.
   */
  public void setBytes(byte[] bytes, int length)
  {
    this.value = bytes;
    this.length = length;
  }

  /**
   * 
   * @param bytes
   *            The buffer now belongs to this class. All the bytes are part of
   *            the current value.
   */
  public void setBytes(byte[] bytes)
  {
    setBytes(bytes, bytes.length);
  }

  /**
   * @param len
   */
  public void ensureCapacity(int len)
  {
    if (len > value.length)
    {
      byte[] newval = new byte[len];
      System.arraycopy(newval, 0, value, 0, length);
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
    //    int c = Item.typeCompare(this, (Writable)x);
    //    if( c != 0 )
    //    {
    //      return c;
    //    }
    byte[] value1 = value;
    JBinary bi = (JBinary) x;
    byte[] value2 = bi.value;
    int len = (length <= bi.length) ? length : bi.length;
    for (int i = 0; i < len; i++)
    {
      int b1 = value1[i] & 0xFF;
      int b2 = value2[i] & 0xFF;
      if (b1 < b2)
      {
        return -1;
      }
      else if (b1 > b2)
      {
        return +1;
      }
    }
    if (length == bi.length)
    {
      return 0;
    }
    else if (length < bi.length)
    {
      return -1;
    }
    else
    {
      return +1;
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    byte[] bs = value;
    int n = length;
    long h = BaseUtil.GOLDEN_RATIO_64;
    for (int i = 0; i < n; i++)
    {
      h |= bs[i];
      h *= BaseUtil.GOLDEN_RATIO_64;
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
    // todo: need to leave long binaries on disk?
    length = BaseUtil.readVUInt(in);
    if (value.length < length)
    {
      value = new byte[length];
    }
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
      out.print(BaseUtil.hexNibble[(b >> 4) & 0x0f]);
      out.print(BaseUtil.hexNibble[b & 0x0f]);
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
      buf.append(BaseUtil.hexNibble[(b >> 4) & 0x0f]);
      buf.append(BaseUtil.hexNibble[b & 0x0f]);
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
  public void copy(JValue jvalue) throws Exception
  {
    JBinary bi = (JBinary) jvalue;
    length = bi.length;
    if (value.length < length)
    {
      value = new byte[length];
    }
    System.arraycopy(bi.value, 0, value, 0, length);
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
