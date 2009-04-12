/*
 * Copyright (C) IBM Corp. 2009.
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public final class BufferedRandomAccessFile extends RandomAccessFile
{
  protected ByteBuffer buffer;
  protected long fileLength;
  protected long fileOffset;

  public BufferedRandomAccessFile(String name, String mode, int bufsize)
    throws IOException
  {
    super(name, mode);
    init(bufsize);
  }

  public BufferedRandomAccessFile(File file, String mode, int bufsize)
    throws IOException
  {
    super(file, mode);
    init(bufsize);
  }
  
  protected void init(int bufsize) throws IOException
  {
    //buffer = ByteBuffer.allocateDirect(bufsize); // TODO: use direct?
    buffer = ByteBuffer.allocate(bufsize);
    buffer.limit(0);
    fileLength = super.length();
    fileOffset = 0;
  }

  @Override
  public long length()
  {
    return fileLength;
  }
  
  @Override
  public long getFilePointer() throws IOException
  {
    // return super.getFilePointer() - buffer.remaining();
    return fileOffset - buffer.remaining();
  }

  @Override
  public void seek(long pos) throws IOException
  {
    buffer.rewind();
    long hi = fileOffset;
    long lo = hi - buffer.remaining();
    if( pos >= lo && pos < hi )
    {
      buffer.position((int)(pos - lo));
    }
    else
    {
      super.seek(pos);
      fileOffset = pos;
      buffer.limit(0);
    }
  }

  
  protected void fillBuffer() throws IOException
  {
    buffer.clear();
    int n = getChannel().read(buffer);
    buffer.position(0);
    if( n < 0 )
    {
      buffer.limit(0);
    }
    else
    {
      buffer.limit(n);
      fileOffset += n;
    }
  }

  @Override
  public int read() throws IOException
  {
    if( buffer.remaining() == 0 )
    {
      fillBuffer();
    }    
    if( buffer.remaining() > 0 )
    {
      int b = buffer.get() & 0xFF;
      return b;
    }
    return -1;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException
  {
    if( buffer.remaining() == 0 )
    {
      fillBuffer();
    }
    if( buffer.remaining() == 0 )
    {
      return -1;
    }
    if( len > buffer.remaining() )
    {
      len = buffer.remaining();
    }
    buffer.get(b, off, len);
    return len;
  }

  @Override
  public int read(byte[] b) throws IOException
  {
    return read(b, 0, b.length);
  }

// TODO: anything to do here?
//  @Override
//  public void setLength(long newLength) throws IOException
//  {
//    super.setLength(newLength);
//  }

  @Override
  public int skipBytes(int n) throws IOException
  {
    long p = getFilePointer();
    seek( p + n );
    n = (int)(getFilePointer() - p);
    return n;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException
  {
    throw new NotImplementedException();
  }

  @Override
  public void write(byte[] b) throws IOException
  {
    throw new NotImplementedException();
  }

  @Override
  public void write(int b) throws IOException
  {
    throw new NotImplementedException();
  }
}
