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
import java.io.PrintStream;
import java.io.Writer;
import java.util.Arrays;

/** 
 * A java.io.PrintWriter replacement that efficiently writes to an underlying Writer
 * without any synchronization.  Buffer is always provided, so its best not to have any
 * buffering in the Writer.  For writing to a char[], FastPrintBuffer is a better choice
 * that using a StringWriter with this class.
 */
public class FastPrintBuffer extends FastPrinter
{
  public FastPrintBuffer()
  {
    this(64*1024);
  }

  public FastPrintBuffer(int bufferSize)
  {
    super(bufferSize);
  }

  @Override
  public void flush()
  {
  }

  @Override
  public void close()
  {
  }

  @Override
  protected void overflow()
  {
    buffer = Arrays.copyOf(buffer, buffer.length << 1);
  }

  @Override
  protected void overflow(char[] buf, int off, int len)
  {
    buffer = Arrays.copyOf(buffer, Math.max(buffer.length << 1, buffer.length + len));
    System.arraycopy(buf, off, buffer, used, len);
    used += len;
  }

  @Override
  protected void overflow(String s, int off, int len)
  {
    buffer = Arrays.copyOf(buffer, Math.max(buffer.length << 1, buffer.length + len));
    s.getChars(off, off + len, buffer, used);
    used += len;
  }
  
  protected void overflow(StringBuffer s, int off, int len)
  {
    buffer = Arrays.copyOf(buffer, Math.max(buffer.length << 1, buffer.length + len));
    s.getChars(off, off + len, buffer, used);
    used += len;
  }

  protected void overflow(StringBuilder s, int off, int len)
  {
    buffer = Arrays.copyOf(buffer, Math.max(buffer.length << 1, buffer.length + len));
    s.getChars(off, off + len, buffer, used);
    used += len;
  }

  
  public String toString()
  {
    return new String(buffer, 0, used);
  }

  public void reset()
  {
    used = 0;
  }

  public final void writeTo(FastPrinter out) throws IOException
  {
    out.write(buffer, 0, used);
  }
  
  public final void writeTo(Writer out) throws IOException
  {
    out.write(buffer, 0, used);
  }
  
  public final void writeTo(PrintStream out)
  {
    out.print(toString());
  }

  /** Returns the internal buffer -- be careful! */
  public final char[] getBuffer()
  {
    return buffer;
  }
  
  /** The number of characters used in the buffer */
  public final int size()
  {
    return used;
  }
}
