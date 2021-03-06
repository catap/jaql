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
import java.nio.channels.FileChannel;


public final class BufferedRandomAccessFile extends RandomAccessFile
{
  protected ByteBuffer buffer;
  protected FileChannel channel;
  protected long fileLength;
  protected long fileOffset;

  public BufferedRandomAccessFile(String name, String mode, int bufsize)
    throws IOException
  {
    super(name, mode);
    init(mode, bufsize);
  }

  public BufferedRandomAccessFile(File file, String mode, int bufsize)
    throws IOException
  {
    super(file, mode);
    init(mode, bufsize);
  }
  
  protected void init(String mode, int bufsize) throws IOException
  {
    buffer = ByteBuffer.allocateDirect(bufsize); // TODO: use direct?
    // buffer = ByteBuffer.allocate(bufsize);
    buffer.limit(0);
    channel = getChannel();
    fileLength = channel.size(); // super.length();
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
      channel.position(pos);  // super.seek(pos);
      fileOffset = pos;
      buffer.limit(0);
    }
  }

  
  protected void fillBuffer() throws IOException
  {
    buffer.clear();
    int n = channel.read(buffer);
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
    throw new RuntimeException("writing not yet implemented");
//    final FileChannel channel = getChannel();
//    int p = buffer.position();
//    int n = buffer.limit();
//    final int c = buffer.capacity();
//    buffer.limit(c);
//    int k = c - n;
//    if( len > k )
//    {
//      long p = fileOffset - n;
//      channel.position(p);
//      fileOffset = p;
//      do
//      {
//        buffer.put(b, off, k);
//        getChannel().write(buffer);
//        len -= k;
//        off += k;
//        fileOffset += k;
//        k = c;
//        buffer.position(0);
//      }
//      while( len > k );
//    }
//    if( len > 0 )
//    {
//      buffer.put(b, off, len);
//    }
//    buffer.limit(buffer.position());
//    buffer.position()
  }

  @Override
  public void write(byte[] b) throws IOException
  {
    throw new RuntimeException("writing not yet implemented");
  }

  @Override
  public void write(int b) throws IOException
  {
    throw new RuntimeException("writing not yet implemented");
  }
}
