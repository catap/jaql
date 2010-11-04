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

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.Writer;


public abstract class FastPrinter extends Writer implements Appendable, Closeable, Flushable
{
  public final static int MIN_BUFFER_SIZE = 32;
  protected final static String lineSeparator = System.getProperty("line.separator", "\n");
  protected final static char[] TRUE = "true".toCharArray();
  protected final static char[] FALSE = "false".toCharArray();
  protected final static char[] NULL = "null".toCharArray();
  

  protected char[] buffer;
  protected int used;


  protected FastPrinter(int bufferSize)
  {
    this.buffer = new char[ Math.min(bufferSize, MIN_BUFFER_SIZE) ];
  }

  public abstract void flush() throws IOException;
  public abstract void close() throws IOException;
    
  protected abstract void overflow() throws IOException;
  
  protected void overflow(char[] buf, int off, int len) throws IOException
  {
    while( len > 0 )
    {
      overflow();
      int r = buffer.length - used;
      if( r < len )
      {
        if( r < 0 )
        {
          throw new IllegalStateException("overflow must make room for at least one character");
        }
      }
      else
      {
        r = len;
      }
      System.arraycopy(buf, off, buffer, used, r);
      off += r;
      len -= r;
    }
  }
  
  protected void overflow(String s, int off, int len) throws IOException
  {
    while( len > 0 )
    {
      overflow();
      int r = buffer.length - used;
      if( r < len )
      {
        if( r < 0 )
        {
          throw new IllegalStateException("overflow must make room for at least one character");
        }
      }
      else
      {
        r = len;
      }
      len -= r;
      r += off;
      s.getChars(off, r, buffer, used);
    }
  }
  
  protected void overflow(StringBuffer s, int off, int len) throws IOException
  {
    while( len > 0 )
    {
      overflow();
      int r = buffer.length - used;
      if( r < len )
      {
        if( r < 0 )
        {
          throw new IllegalStateException("overflow must make room for at least one character");
        }
      }
      else
      {
        r = len;
      }
      len -= r;
      r += off;
      s.getChars(off, r, buffer, used);
    }
  }
  
  
  protected void overflow(StringBuilder s, int off, int len) throws IOException
  {
    while( len > 0 )
    {
      overflow();
      int r = buffer.length - used;
      if( r < len )
      {
        if( r < 0 )
        {
          throw new IllegalStateException("overflow must make room for at least one character");
        }
      }
      else
      {
        r = len;
      }
      len -= r;
      r += off;
      s.getChars(off, r, buffer, used);
    }
  }

  
  public final void write(int c) throws IOException
  {
    if( used >= buffer.length )
    {
      overflow();
    }
    buffer[used++] = (char)c;
  }

  
  public final void write(char[] buf, int off, int len) throws IOException
  {
    if( len < buffer.length - used )
    {
      System.arraycopy(buf, off, buffer, used, len);
      used += len;
    }
    else
    {
      overflow(buf, off, len);
    }
  }

  
  public final void write(char[] buf) throws IOException
  {
    write(buf, 0, buf.length);
  }

    /**
     * Writes a portion of a string.
     * @param s A String
     * @param off Offset from which to start writing characters
     * @param len Number of characters to write
     */
  public final void write(String s, int off, int len) throws IOException
  {
    if( len < buffer.length - used )
    {
      s.getChars(off, off + len, buffer, used);
      used += len;
    }
    else
    {
      overflow(s, off, len);
    }
  }
  
  public final void write(StringBuffer s, int off, int len) throws IOException
  {
    if( len < buffer.length - used )
    {
      s.getChars(off, off + len, buffer, used);
      used += len;
    }
    else
    {
      overflow(s, off, len);
    }
  }

  public final void write(StringBuilder s, int off, int len) throws IOException
  {
    if( len < buffer.length - used )
    {
      s.getChars(off, off + len, buffer, used);
      used += len;
    }
    else
    {
      overflow(s, off, len);
    }
  }


  public final void write(String s) throws IOException
  {
    write(s, 0, s.length());
  }

  public final void write(StringBuffer s) throws IOException
  {
    write(s, 0, s.length());
  }

  public final void write(StringBuilder s) throws IOException
  {
    write(s, 0, s.length());
  }

  public final void print(boolean x) throws IOException
  {
    write(x ? TRUE : FALSE);
  }

  public final void print(char x) throws IOException
  {
    write(x);
  }

  public final void print(int x) throws IOException
  {
    // TODO: replace all print routines with conversion that do NOT allocate any memory
    write(Integer.toString(x));
  }

  public final void print(long x) throws IOException
  {
    // TODO: replace all print routines with conversion that do NOT allocate any memory
    write(Long.toString(x));
  }

  public final void print(float x) throws IOException
  {
    // TODO: replace all print routines with conversion that do NOT allocate any memory
    write(Float.toString(x));
  }

  public final void print(double x) throws IOException
  {
    // TODO: replace all print routines with conversion that do NOT allocate any memory
    write(Double.toString(x));
  }

  public final void print(char[] x) throws IOException
  {
    write(x);
  }

  public final void print(String s) throws IOException
  {
    if( s == null ) write(NULL);
    else write(s);
  }

  public final void print(Object obj) throws IOException
  {
    if( obj == null ) write(NULL);
    else write(obj.toString());
  }

  
  public final void println() throws IOException
  {
    write(lineSeparator);
  }

  public final void println(boolean x) throws IOException
  {
    print(x);
    println();
  }

  public final void println(char x) throws IOException
  {
    print(x);
    println();
  }

  public final void println(int x) throws IOException
  {
    print(x);
    println();
  }

  public final void println(long x) throws IOException
  {
    print(x);
    println();
  }

  public final void println(float x) throws IOException
  {
    print(x);
    println();
  }

  public final void println(double x) throws IOException
  {
    print(x);
    println();
  }

  public final void println(char[] x) throws IOException
  {
    print(x);
    println();
  }

  public final void println(String x) throws IOException
  {
    print(x);
    println();
  }

  public final void println(Object x) throws IOException
  {
    print(x);
    println();
  }


  public final FastPrinter append(CharSequence c) throws IOException
  {
    if( c == null ) write(NULL);
    else write(c.toString());
    return this;
  }

  public final FastPrinter append(CharSequence c, int start, int end) throws IOException
  {
    if( c == null ) write(NULL);
    else write(c.subSequence(start,end).toString());
    return this;
  }
    
  public final FastPrinter append(char c) throws IOException
  {
    write(c);
    return this;
  }

  // TODO: support print formats?
  // public FastPrintWriter printf(String format, Object ... args)
  // public FastPrintWriter printf(Locale l, String format, Object ... args)
  // public FastPrintWriter format(String format, Object ... args) 
  // public FastPrintWriter format(Locale l, String format, Object ... args)
}
