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
import java.io.UnsupportedEncodingException;
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.json.util.JsonUtil;
import com.ibm.jaql.util.BaseUtil;

/**
 * 
 */
public class JString extends JAtom
{
  protected final static byte[] NO_BYTES = new byte[0];

  protected byte[]              bytes    = NO_BYTES;
  protected int                 len;
  protected String              stringCache;

  /**
   * 
   */
  public JString()
  {
  }

  /**
   * Construct from a string.
   */
  public JString(String string)
  {
    set(string);
  }

  /**
   * Construct from another JString.
   * 
   * @param string
   */
  public JString(JString string)
  {
    setCopy(string);
  }

  /**
   * Construct from a byte array.
   * 
   * @param utf8
   */
  public JString(byte[] utf8)
  {
    set(utf8, utf8.length);
  }

  /**
   * @param utf8
   * @param len
   */
  public JString(byte[] utf8, int len)
  {
    set(utf8, len);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.STRING;
  }

  /**
   * @param maxlen
   */
  public void setCapacity(int maxlen)
  {
    if (maxlen > bytes.length)
    {
      byte[] b = new byte[maxlen];
      System.arraycopy(bytes, 0, b, 0, len);
      bytes = b;
    }
  }

  /**
   * array now belongs to this class
   * 
   * @param utf8
   * @param len
   */
  public void set(byte[] utf8, int len)
  {
    this.bytes = utf8;
    this.len = len;
    this.stringCache = null;
  }

  /**
   * array now belongs to this class
   * 
   * @param utf8
   */
  public void set(byte[] utf8)
  {
    set(utf8, utf8.length);
  }

  /**
   * @param string
   */
  public void set(String string)
  {
    try
    {
      this.stringCache = string;
      bytes = string.getBytes("UTF8");
      len = bytes.length;
    }
    catch (UnsupportedEncodingException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /**
   * @param string
   */
  public void setCopy(JString string)
  {
    if (bytes.length < string.len)
    {
      bytes = new byte[string.len];
    }
    System.arraycopy(string.bytes, 0, bytes, 0, string.len);
    len = string.len;
    stringCache = string.stringCache;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JValue value) throws Exception
  {
    setCopy((JString) value);
  }

  /**
   * The utf8 bytes are copied.
   * 
   * @param utf8
   * @param offset
   * @param len
   */
  public void setCopy(byte[] utf8, int offset, int len)
  {
    if (bytes.length < len)
    {
      bytes = new byte[len];
    }
    System.arraycopy(utf8, offset, bytes, 0, len);
    this.len = len;
    this.stringCache = null;
  }

  /**
   * The utf8 bytes are copied.
   * 
   * @param utf8
   * @param len
   */
  public void setCopy(byte[] utf8, int len)
  {
    setCopy(utf8, 0, len);
  }

  /**
   * The utf8 bytes are copied.
   * 
   * @param utf8
   */
  public void setCopy(byte[] utf8)
  {
    setCopy(utf8, 0, utf8.length);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object x)
  {
    JString s = (JString) x;
    int len = this.len;
    if (s.len < len)
    {
      len = s.len;
    }
    for (int i = 0; i < len; i++)
    {
      int c = (int) (bytes[i] & 0xff) - (int) (s.bytes[i] & 0xff);
      if (c != 0)
      {
        return c;
      }
    }
    int c = this.len - s.len;
    return c;
  }
  
  public int hashCode() {
  	try {
  		return toString().hashCode(); // this way, hash code is cached in stringCache
  	}	catch (UndeclaredThrowableException e) {
  		// standard hash code
  		int result = 1;
  		for (byte element : bytes) {
  			result = 31 * result + element;
  		}
  		return result;
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
    // TODO: inefficient; use caching similar to java.lang.String
  	byte[] bs = bytes;
    int n = len;
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
   * @see com.ibm.jaql.json.type.JAtom#print(java.io.PrintStream)
   */
  @Override
  public void print(PrintStream out)
  {
    JsonUtil.printQuoted(out, this);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toJSON()
   */
  @Override
  public String toJSON()
  {
    String s = toString();
    s = JsonUtil.quote(s);
    return s;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toString()
   */
  @Override
  public String toString()
  {
    try
    {
      if (stringCache == null)
      {
        stringCache = new String(bytes, 0, len, "UTF8");
      }
      return stringCache;
    }
    catch (UnsupportedEncodingException e)
    {
      throw new UndeclaredThrowableException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataInput)
   */
  public void readFields(DataInput in) throws IOException
  {
    stringCache = null;
    len = BaseUtil.readVUInt(in);
    if (bytes.length < len)
    {
      bytes = new byte[len];
    }
    in.readFully(bytes, 0, len);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    BaseUtil.writeVUInt(out, len);
    out.write(bytes, 0, len);
  }

  /**
   * 
   * @return the number of utf8 bytes (not number of characters!)
   */
  public int getLength()
  {
    return len;
  }

  /**
   * @param prefix
   * @return
   */
  public boolean startsWith(JString prefix)
  {
    int n = prefix.len;
    if (len < n)
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

  /**
   * You should not modify the bytes (or toString() might not work). The bytes
   * still belong to this class.
   * 
   * @return
   */
  public byte[] getBytes()
  {
    return bytes;
  }

  /**
   * @param in
   * @return
   * @throws IOException
   */
  public static String readString(DataInput in) throws IOException
  {
    JString js = new JString(); // TODO: memory
    js.readFields(in);
    return js.toString();
  }

  /**
   * @param out
   * @param string
   * @throws IOException
   */
  public static void writeString(DataOutput out, String string)
      throws IOException
  {
    JString js = new JString(string); // TODO: memory
    js.write(out);
  }

  /**
   * @param offset
   * @param length
   */
  public void removeBytes(int offset, int length)
  {
    final int n = this.len;
    if (offset < 0 || offset >= n || length <= 0)
    {
      // out of range
      return;
    }
    int i = offset + length;
    if (i >= n || i < 0)
    {
      // removing the tail
      len = offset;
      return;
    }
    for (; i < len; offset++, i++)
    {
      bytes[offset] = bytes[i];
    }
    len -= length;
    stringCache = null;
  }

  /**
   * Replace the first occurence of oldc with newc. This only works on 7-bit
   * ascii values right now!
   * 
   * @param oldc
   * @param newc
   */
  public void replace(char oldc, char newc)
  {
    for (int i = 0; i < len; i++)
    {
      if (bytes[i] == oldc)
      {
        bytes[i] = (byte) newc;
        break;
      }
    }
  }

  /**
   * Find the first occurence of c. If found, return the byte index else return
   * -1. This only works on 7-bit ascii values right now!
   * 
   * @param c
   * @return
   */
  public int indexOf(char c)
  {
    for (int i = 0; i < len; i++)
    {
      if (bytes[i] == c)
      {
        return i;
      }
    }
    return -1;
  }

  
}
