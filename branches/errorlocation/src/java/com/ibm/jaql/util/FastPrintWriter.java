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
import java.io.Writer;

/** 
 * A java.io.PrintWriter replacement that efficiently writes to an underlying Writer
 * without any synchronization.  Buffer is always provided, so its best not to have any
 * buffering in the Writer.  For writing to a char[], FastPrintBuffer is a better choice
 * that using a StringWriter with this class.
 */
public class FastPrintWriter extends FastPrinter
{
  protected Writer out;

  public FastPrintWriter(Writer out)
  {
    this(out, 64*1024);
  }

  public FastPrintWriter(Writer out, int bufferSize)
  {
    super(bufferSize);
    this.out = out;
  }

  @Override
  public void flush() throws IOException
  {
    if( used > 0 )
    {
      out.write(buffer, 0, used);
      out.flush();
      used = 0;
    }
  }

  @Override
  public void close() throws IOException
  {
    flush();
    out.close();
  }

  @Override
  protected void overflow() throws IOException
  {
    out.write(buffer, 0, used);
    used = 0;
  }

  @Override
  protected void overflow(char[] buf, int off, int len) throws IOException
  {
    out.write(buffer, 0, used);
    if( len > (buffer.length >> 2) )
    {
      out.write(buf, off, len);
      used = 0;
    }
    else
    {
      System.arraycopy(buf, off, buffer, 0, len);
      used = len;
    }
  }

  @Override
  protected void overflow(String s, int off, int len) throws IOException
  {
    out.write(buffer, 0, used);
    if( len > (buffer.length >> 2) )
    {
      out.write(s, off, len);
      used = 0;
    }
    else
    {
      s.getChars(off, off + len, buffer, 0);
      used = len;
    }
  }

  @Override
  protected void overflow(StringBuilder s, int off, int len) throws IOException
  {
    out.write(buffer, 0, used);
    if( len > (buffer.length >> 2) )
    {
      out.append(s, off, off + len);
      used = 0;
    }
    else
    {
      s.getChars(off, off + len, buffer, 0);
      used = len;
    }
  }

  @Override
  protected void overflow(StringBuffer s, int off, int len) throws IOException
  {
    out.write(buffer, 0, used);
    if( len > (buffer.length >> 2) )
    {
      out.append(s, off, off + len);
      used = 0;
    }
    else
    {
      s.getChars(off, off + len, buffer, 0);
      used = len;
    }
  }
}
