/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/** Like ByteArrayOutputStream, but unsynchronzied */
public class UnsynchronizedByteArrayOutputStream extends RandomAccessBuffer
{
  public UnsynchronizedByteArrayOutputStream()
  {
  }

  @Override
  public void write(int b)
  {
    if (count >= buf.length)
    {
      buf = Arrays.copyOf(buf, buf.length*2);
    }
    buf[count++] = (byte) b;
  }

  @Override
  public void write(byte b[], int off, int len)
  {
    if (count + len >= buf.length)
    {
      buf = Arrays.copyOf(buf, Math.min(buf.length*2, count+len));
    }
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }
  
  @Override
  public void writeTo(OutputStream out) throws IOException
  {
    out.write(buf, 0, count);
  }


  @Override
  public void reset()
  {
    count = 0;
  }

  @Override
  public byte[] toByteArray()
  {
    return Arrays.copyOf(buf, count);
  }

  /** like toByteArray() but doesn't make a copy -- be careful! */
  public byte[] getBuffer()
  {
    return buf;
  }
  
  @Override
  public int size()
  {
    return count;
  }
  
  @Override
  public String toString() 
  {
    return new String(buf, 0, count);
  }

  @Override
  public String toString(String charsetName) throws UnsupportedEncodingException
  {
    return new String(buf, 0, count, charsetName);
  }

}
