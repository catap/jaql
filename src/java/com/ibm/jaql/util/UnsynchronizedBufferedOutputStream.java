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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;


/** Like {@link BufferedOutputStream} but without synchronization. */
public class UnsynchronizedBufferedOutputStream extends OutputStream
{
  static final int BUF_SIZE = 65768;  
  byte[] buf = new byte[BUF_SIZE];
  int count = 0;
  OutputStream out;

  public UnsynchronizedBufferedOutputStream(OutputStream out) {
    this.out = out;
  }

  @Override
  public void write(int b) throws IOException
  {
    if (count == buf.length)
    {
      flushBuf();
    }
    buf[count++] = (byte) b;
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException
  {
    // does not fit
    if (count + len > buf.length)
    {
      flushBuf(); // could be optimized but probably not worth it
      if (len >= buf.length)
      {
        out.write(b, off, len);
        return;
      }
    }

    // fits
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  /** Flushes the internal buffer but does not flush the underlying output stream */
  public void flushBuf() throws IOException
  {
    if (count > 0)
    {
      try
      {
        out.write(buf, 0, count);
      } catch (IOException e)
      {
        throw new RuntimeException(e);
      }
      count = 0;
    }
  }

  @Override
  public void flush() throws IOException {
    flushBuf();
    out.flush();
  }
  
  @Override
  public void close() throws IOException {
    flushBuf();
    out.close();      
  }
}  
